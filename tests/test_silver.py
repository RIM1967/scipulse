"""Tests de la couche Silver : clés de jointure et nettoyage des trois sources."""

from __future__ import annotations

import pandas as pd
import pytest

from scipulse.silver import arxiv, citations, hn
from scipulse.silver.keys import (
    arxiv_doi,
    arxiv_id_from_doi,
    arxiv_id_from_url,
    normalize_arxiv_id,
    normalize_doi,
)

# ── Clés de jointure ─────────────────────────────────────────────────────────

@pytest.mark.parametrize(("raw", "expected"), [
    ("2003.01234v2", "2003.01234"),
    (" 1706.03762 ", "1706.03762"),
    ("cs/0701001", "cs/0701001"),
    ("math.CO/0601001v1", "math.CO/0601001"),
    ("pas-un-id", None),
    (None, None),
])
def test_normalize_arxiv_id(raw: str | None, expected: str | None) -> None:
    assert normalize_arxiv_id(raw) == expected


@pytest.mark.parametrize(("url", "expected"), [
    ("https://arxiv.org/abs/1706.03762", "1706.03762"),
    ("https://arxiv.org/pdf/2003.01234v3.pdf", "2003.01234"),
    ("http://www.arxiv.org/abs/cs/0701001v2", "cs/0701001"),
    ("https://arxiv.org/html/2401.00001v1", "2401.00001"),
    ("https://github.com/foo/bar", None),
    (None, None),
])
def test_arxiv_id_from_url(url: str | None, expected: str | None) -> None:
    assert arxiv_id_from_url(url) == expected


@pytest.mark.parametrize(("raw", "expected"), [
    ("https://doi.org/10.1145/ABC.123", "10.1145/abc.123"),
    ("doi:10.1000/XYZ", "10.1000/xyz"),
    # Anomalies réelles du dump ArXiv
    ("10.1007/978-3-031-13321-3\\_14", "10.1007/978-3-031-13321-3_14"),
    ("10.1007/a-1 10.1007/b-2", "10.1007/a-1"),
    ("pas un doi", None),
    ("", None),
])
def test_normalize_doi(raw: str, expected: str | None) -> None:
    assert normalize_doi(raw) == expected


def test_arxiv_doi_aller_retour() -> None:
    assert arxiv_doi("1706.03762") == "10.48550/arxiv.1706.03762"
    assert arxiv_id_from_doi("https://doi.org/10.48550/arXiv.1706.03762") == "1706.03762"
    assert arxiv_id_from_doi("10.1145/123") is None


# ── ArXiv ────────────────────────────────────────────────────────────────────

def raw_paper(**overrides: object) -> dict:
    paper = {
        "id": "1706.03762", "title": "  Attention Is\n All You Need ", "abstract": "x " * 60,
        "authors_parsed": [["Vaswani", "Ashish", ""], ["Shazeer", "Noam", ""]],
        "categories": "cs.CL cs.LG", "versions": [{"created": "Mon, 12 Jun 2017 17:57:34 GMT"}],
        "update_date": "2023-08-02", "doi": None,
    }
    paper.update(overrides)
    return paper


def test_clean_record_arxiv() -> None:
    row = arxiv.clean_record(raw_paper())
    assert row is not None
    assert row["title"] == "Attention Is All You Need"
    assert row["authors"] == ["Ashish Vaswani", "Noam Shazeer"] and row["n_authors"] == 2
    assert row["primary_category"] == "cs.CL"
    assert row["category_name"] == "Computation and Language"
    assert row["published_year"] == 2017
    assert row["arxiv_doi"] == "10.48550/arxiv.1706.03762"


def test_clean_record_rejette_sans_titre_ou_id() -> None:
    assert arxiv.clean_record(raw_paper(title="  ")) is None
    assert arxiv.clean_record(raw_paper(id="???")) is None


def test_build_silver_arxiv_deduplique_par_mise_a_jour() -> None:
    old = raw_paper(update_date="2020-01-01", title="Ancien titre")
    new = raw_paper(update_date="2023-01-01", title="Nouveau titre")
    df, rejected = arxiv.build_silver([old, new, raw_paper(id="invalide")])
    assert rejected == 1 and len(df) == 1
    assert df.loc[0, "title"] == "Nouveau titre"


# ── Hacker News ──────────────────────────────────────────────────────────────

def envelope(source: str, ingested_at: str, **payload: object) -> dict:
    return {"_source": source, "_ingested_at": ingested_at, "payload": payload}


def test_build_silver_hn_unifie_et_garde_le_dernier_snapshot() -> None:
    envelopes = [
        envelope("firebase", "2026-01-01T00:00:00+00:00", id=1, title="Old", score=5,
                 descendants=None, url="https://arxiv.org/abs/1706.03762", time=1_700_000_000),
        envelope("firebase", "2026-01-02T00:00:00+00:00", id=1, title="New", score=50,
                 descendants=3, url="https://arxiv.org/abs/1706.03762", time=1_700_000_000),
        envelope("algolia", "2026-01-01T00:00:00+00:00", objectID="2", title="Algolia",
                 points=None, num_comments=7, url=None, created_at_i=1_600_000_000),
        {"_source": "inconnu", "payload": {}},
    ]
    df, rejected = hn.build_silver(envelopes)
    assert rejected == 1 and list(df["hn_id"]) == [1, 2]
    first = df.iloc[0]
    assert first["score"] == 50 and first["num_comments"] == 3       # snapshot le plus récent
    assert first["arxiv_id"] == "1706.03762"
    assert df.iloc[1]["score"] == 0                                    # valeur manquante → 0
    assert pd.isna(df.iloc[1]["arxiv_id"])


# ── Citations OpenAlex ───────────────────────────────────────────────────────

def test_clean_work_openalex() -> None:
    work = {
        "id": "https://openalex.org/W2626778328",
        "doi": "https://doi.org/10.48550/arXiv.1706.03762",
        "title": "Attention Is All You Need", "publication_year": 2017,
        "cited_by_count": 78211, "counts_by_year": [{"year": 2025, "cited_by_count": 9000}],
        "referenced_works": ["https://openalex.org/W1"], "fwci": 120.5,
        "primary_topic": {"display_name": "Topic", "field": {"display_name": "Computer Science"}},
    }
    row = citations.clean_work(work)
    assert row is not None
    assert row["openalex_id"] == "W2626778328" and row["arxiv_id"] == "1706.03762"
    assert row["doi"] == "10.48550/arxiv.1706.03762"
    assert row["referenced_works"] == ["W1"] and row["topic_field"] == "Computer Science"


def test_build_silver_citations_rejette_sans_doi_et_deduplique() -> None:
    work = {"id": "https://openalex.org/W1", "doi": "10.1/a", "cited_by_count": 1}
    df, rejected = citations.build_silver([work, work, {"id": "https://openalex.org/W2"}])
    assert rejected == 1 and len(df) == 1
