"""Bronze Hacker News : deux flux complémentaires vers MinIO `hn-raw/`.

1. **Poller micro-batch** (toutes les 5 min, API Firebase officielle) :
   capture les stories récentes et populaires. Les stories déjà vues sont
   re-capturées tant qu'elles sont dans les listes : chaque fichier est un
   *snapshot* horodaté, et Silver conserve le plus récent (score à jour).

2. **Backfill historique** (API Algolia, à la demande) : toutes les stories
   dont l'URL pointe vers arxiv.org. Sans lui, le recouvrement ArXiv × HN
   serait quasi nul : le poller ne voit que les dernières heures, alors que
   le corpus ArXiv couvre plusieurs années.

Chaque enregistrement est enveloppé sans être modifié :
    {"_source": ..., "_ingested_at": ..., "payload": <réponse brute de l'API>}
L'enveloppe ajoute la traçabilité (provenance, date) exigée pour l'audit,
tout en gardant la réponse source intacte dans `payload`.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import requests

from scipulse.config import Bucket, Settings
from scipulse.http import make_session
from scipulse.monitoring import Layer, MetricsRecorder
from scipulse.storage import DataLake, partition_key

FIREBASE = "https://hacker-news.firebaseio.com/v0"
ALGOLIA = "https://hn.algolia.com/api/v1/search_by_date"
POLLED_LISTS = ("topstories", "newstories", "beststories")


def envelope(source: str, payload: dict[str, Any], ingested_at: datetime) -> dict[str, Any]:
    return {"_source": source, "_ingested_at": ingested_at.isoformat(), "payload": payload}


# ── 1. Poller Firebase ───────────────────────────────────────────────────────

def fetch_story_ids(session: requests.Session, lists: tuple[str, ...] = POLLED_LISTS) -> list[int]:
    """Union dédupliquée des identifiants des listes HN (≈ 500 ids chacune)."""
    ids: set[int] = set()
    for name in lists:
        response = session.get(f"{FIREBASE}/{name}.json", timeout=15)
        response.raise_for_status()
        ids.update(response.json() or [])
    return sorted(ids)


def fetch_items(session: requests.Session, ids: list[int], workers: int = 16
                ) -> list[dict[str, Any]]:
    """Récupère les items en parallèle (I/O bound → threads). Ignore les items supprimés."""
    def fetch(item_id: int) -> dict[str, Any] | None:
        response = session.get(f"{FIREBASE}/item/{item_id}.json", timeout=15)
        response.raise_for_status()
        return response.json()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        items = list(pool.map(fetch, ids))
    return [i for i in items
            if i and i.get("type") == "story" and not i.get("deleted") and not i.get("dead")]


def poll(settings: Settings | None = None) -> int:
    """Point d'entrée du DAG micro-batch."""
    settings = settings or Settings.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="hn", layer=Layer.BRONZE, source="hn",
                        task="poll_hn_bronze") as metrics:
        session = make_session()
        ids = fetch_story_ids(session)
        stories = fetch_items(session, ids)
        run_at = datetime.now(UTC)
        metrics.records_in = len(ids)
        metrics.records_out = lake.put_records(
            Bucket.HN_RAW, partition_key("firebase", run_at, 0, "jsonl"),
            (envelope("firebase", s, run_at) for s in stories),
        )
        if stories:
            metrics.data_watermark = datetime.fromtimestamp(
                max(s.get("time", 0) for s in stories), UTC).isoformat()
    return metrics.records_out


# ── 2. Backfill Algolia ──────────────────────────────────────────────────────

def iter_arxiv_stories(session: requests.Session, since: datetime,
                       hits_per_page: int = 1000, max_pages: int = 500
                       ) -> Iterator[dict[str, Any]]:
    """Parcourt toutes les stories HN dont l'URL contient arxiv.org, de la plus récente
    à `since`.

    Algolia plafonne une recherche à 1000 résultats : on pagine donc par
    *fenêtre temporelle glissante* (created_at_i < plus ancien vu) au lieu de
    `page=N`, ce qui permet de remonter tout l'historique.
    """
    upper = int(datetime.now(UTC).timestamp()) + 1
    lower = int(since.timestamp())
    for _ in range(max_pages):
        response = session.get(ALGOLIA, timeout=30, params={
            "query": "arxiv.org",
            "restrictSearchableAttributes": "url",
            "tags": "story",
            "hitsPerPage": hits_per_page,
            "numericFilters": f"created_at_i<{upper},created_at_i>={lower}",
        })
        response.raise_for_status()
        hits = response.json().get("hits", [])
        if not hits:
            return
        yield from hits
        oldest = min(h["created_at_i"] for h in hits)
        if oldest >= upper:   # garde-fou : la fenêtre doit toujours reculer
            return
        upper = oldest


def backfill(settings: Settings | None = None, since: datetime | None = None,
             chunk_size: int = 5000) -> int:
    """Point d'entrée du DAG de backfill (déclenchement manuel, une fois)."""
    settings = settings or Settings.from_env()
    since = since or datetime(2015, 1, 1, tzinfo=UTC)
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="hn", layer=Layer.BRONZE, source="hn",
                        task="backfill_hn_algolia") as metrics:
        run_at = datetime.now(UTC)
        buffer: list[dict[str, Any]] = []
        part = written = 0
        for hit in iter_arxiv_stories(make_session(), since):
            buffer.append(envelope("algolia", hit, run_at))
            if len(buffer) >= chunk_size:
                written += lake.put_records(
                    Bucket.HN_RAW, partition_key("algolia", run_at, part, "jsonl"), buffer)
                buffer, part = [], part + 1
        if buffer:
            written += lake.put_records(
                Bucket.HN_RAW, partition_key("algolia", run_at, part, "jsonl"), buffer)
        metrics.records_in = metrics.records_out = written
        metrics.extra = {"since": since.isoformat()}
    return written
