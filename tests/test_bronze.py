"""Tests de la couche Bronze (ArXiv, Hacker News). Aucun accès réseau ni MinIO réel."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fakes import FakeSession

from scipulse.bronze import arxiv, hn
from scipulse.config import Bucket, Settings
from scipulse.storage import DataLake


def arxiv_line(paper_id: str, categories: str, created: str) -> str:
    return json.dumps({"id": paper_id, "categories": categories,
                       "versions": [{"version": "v1", "created": created}]})


RECENT = "Mon, 2 Mar 2020 10:00:00 GMT"
OLD = "Mon, 2 Apr 2007 19:18:42 GMT"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(metrics_dir=tmp_path / "metrics", data_dir=tmp_path / "data")


@pytest.fixture
def use_lake(lake: DataLake, monkeypatch: pytest.MonkeyPatch) -> DataLake:
    """Fait pointer les modules Bronze sur le S3 simulé au lieu de MinIO."""
    monkeypatch.setattr(DataLake, "from_settings", classmethod(lambda cls, s: lake))
    return lake


# ── ArXiv ────────────────────────────────────────────────────────────────────

def test_submission_year() -> None:
    assert arxiv.submission_year(json.loads(arxiv_line("1", "cs.AI", OLD))) == 2007
    assert arxiv.submission_year({"versions": []}) is None


@pytest.mark.parametrize(("categories", "created", "expected"), [
    ("cs.LG stat.ML", RECENT, True),
    ("stat.ML cs.LG", RECENT, True),     # cs.* en catégorie secondaire : retenu
    ("math.CO", RECENT, False),
    ("cs.AI", OLD, False),               # avant min_year
])
def test_is_eligible(categories: str, created: str, expected: bool) -> None:
    record = json.loads(arxiv_line("1", categories, created))
    assert arxiv.is_eligible(record, arxiv.SamplingConfig(min_year=2015)) is expected


def test_reservoir_sample_taille_et_eligibilite() -> None:
    lines = [arxiv_line(str(i), "cs.AI", RECENT) for i in range(500)]
    lines += [arxiv_line(f"m{i}", "math.CO", RECENT) for i in range(100)] + ["{corrompu"]
    sample, read, eligible = arxiv.reservoir_sample(lines, arxiv.SamplingConfig(sample_size=50))
    assert (len(sample), read, eligible) == (50, 601, 500)
    assert all("cs.AI" in line for line in sample)


def test_reservoir_sample_reproductible() -> None:
    lines = [arxiv_line(str(i), "cs.AI", RECENT) for i in range(1000)]
    config = arxiv.SamplingConfig(sample_size=10, seed=7)
    assert arxiv.reservoir_sample(lines, config)[0] == arxiv.reservoir_sample(lines, config)[0]


def test_arxiv_run_ecrit_les_lignes_intactes(settings: Settings, use_lake: DataLake) -> None:
    dump = settings.data_dir / "arxiv" / arxiv.DUMP_NAME
    dump.parent.mkdir(parents=True)
    lines = [arxiv_line(f"2003.{i:05d}", "cs.CL", RECENT) for i in range(25)]
    dump.write_text("\n".join(lines) + "\n", encoding="utf-8")

    written = arxiv.run(settings, arxiv.SamplingConfig(sample_size=20, chunk_size=8))

    keys = use_lake.list_keys(Bucket.ARXIV_RAW, "arxiv/")
    assert written == 20 and len(keys) == 3            # 8 + 8 + 4
    stored = [json.dumps(r) for r in use_lake.iter_records(Bucket.ARXIV_RAW, "arxiv/")]
    assert set(stored) <= set(lines)                   # octet pour octet
    metric = json.loads(next((settings.metrics_dir).glob("*.jsonl")).read_text())
    assert metric["records_out"] == 20 and metric["extra"]["eligible"] == 25


# ── Hacker News ──────────────────────────────────────────────────────────────

ITEMS = {
    1: {"id": 1, "type": "story", "title": "A", "score": 10, "time": 1_700_000_000},
    2: {"id": 2, "type": "comment", "text": "x"},
    3: {"id": 3, "type": "story", "deleted": True},
    4: None,
}


def firebase_handler(url: str, _params: dict) -> object:
    if url.endswith("stories.json"):
        return [1, 2, 3, 4]
    return ITEMS[int(url.rsplit("/", 1)[1].removesuffix(".json"))]


def test_fetch_items_filtre_les_non_stories() -> None:
    session = FakeSession(firebase_handler)
    stories = hn.fetch_items(session, hn.fetch_story_ids(session))
    assert [s["id"] for s in stories] == [1]


def test_poll_ecrit_une_enveloppe(settings: Settings, use_lake: DataLake,
                                  monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hn, "make_session", lambda: FakeSession(firebase_handler))
    assert hn.poll(settings) == 1
    [record] = list(use_lake.iter_records(Bucket.HN_RAW, "firebase/"))
    assert record["_source"] == "firebase"
    assert record["payload"] == ITEMS[1]               # réponse API intacte


def test_algolia_fenetre_glissante() -> None:
    pages = [
        [{"objectID": "a", "created_at_i": 300}, {"objectID": "b", "created_at_i": 200}],
        [{"objectID": "c", "created_at_i": 100}],
        [],
    ]
    session = FakeSession(lambda url, params: {"hits": pages.pop(0)})
    hits = list(hn.iter_arxiv_stories(session, datetime(2015, 1, 1, tzinfo=UTC)))
    assert [h["objectID"] for h in hits] == ["a", "b", "c"]
    # la borne haute recule jusqu'au plus ancien résultat de la page précédente
    assert "created_at_i<200," in session.calls[1][1]["numericFilters"]
    assert "created_at_i<100," in session.calls[2][1]["numericFilters"]


def test_backfill_decoupe_en_fichiers(settings: Settings, use_lake: DataLake,
                                      monkeypatch: pytest.MonkeyPatch) -> None:
    hits = [{"objectID": str(i), "created_at_i": 1000 - i} for i in range(7)]
    pages = [hits, []]
    monkeypatch.setattr(hn, "make_session",
                        lambda: FakeSession(lambda url, params: {"hits": pages.pop(0)}))
    assert hn.backfill(settings, chunk_size=3) == 7
    assert len(use_lake.list_keys(Bucket.HN_RAW, "algolia/")) == 3   # 3 + 3 + 1
