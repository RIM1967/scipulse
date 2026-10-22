"""Bronze citations : OpenAlex → MinIO `citations-raw/` (JSON Lines).

Pourquoi OpenAlex plutôt que Semantic Scholar (justification README) :
  - accès libre sans clé, quota « polite pool » de 10 req/s avec un e-mail ;
  - filtre par lot : jusqu'à 100 DOI par requête (`doi:a|b|c`) → ~2 000
    requêtes pour 100 000 articles, contre 1 requête/article ailleurs ;
  - données riches pour le score : `counts_by_year` (vélocité), `fwci`,
    `referenced_works` (graphe de citations → PageRank, option B),
    `primary_topic` (champ disciplinaire) ;
  - licence CC0 : réutilisation sans contrainte.

Stratégie de correspondance : pour chaque article on interroge son DOI
ArXiv (10.48550/arxiv.<id>) **et**, s'il existe, le DOI de la version
publiée en revue. OpenAlex fusionne souvent preprint et article publié sous
le DOI éditeur : interroger les deux maximise le taux de correspondance.
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

API = "https://api.openalex.org/works"
FIELDS = ("id,doi,title,publication_year,type,cited_by_count,counts_by_year,"
          "fwci,primary_topic,referenced_works")
# Maximum accepté par OpenAlex dans un filtre OR. Chaque requête coûte
# 0,0001 $ de quota : budget gratuit 0,10 $/jour sans clé (~1 000 requêtes,
# partagé par IP), 1 $/jour avec une clé gratuite (~10 000 requêtes).
DOIS_PER_REQUEST = 100


def batches(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def fetch_batch(session: requests.Session, dois: list[str], settings: Settings
                ) -> list[dict[str, Any]]:
    """Une requête OpenAlex pour un lot de DOI (filtre OR)."""
    params: dict[str, Any] = {
        "filter": "doi:" + "|".join(f"https://doi.org/{d}" for d in dois),
        "select": FIELDS,
        # Un même DOI peut renvoyer plusieurs works (doublons OpenAlex) : marge
        "per-page": DOIS_PER_REQUEST * 2,
    }
    if settings.openalex_mailto:
        params["mailto"] = settings.openalex_mailto
    if settings.openalex_api_key:
        params["api_key"] = settings.openalex_api_key
    response = session.get(API, params=params, timeout=60)
    if response.status_code == 400:
        # Un DOI malformé fait rejeter tout le lot : on coupe en deux pour
        # l'isoler, et on n'abandonne que lui (O(log n) requêtes en plus)
        if len(dois) == 1:
            return []
        middle = len(dois) // 2
        return (fetch_batch(session, dois[:middle], settings)
                + fetch_batch(session, dois[middle:], settings))
    response.raise_for_status()
    return response.json().get("results", [])


def target_dois(lake: DataLake) -> list[str]:
    """DOI à interroger, lus depuis Silver ArXiv (déjà normalisés)."""
    df = lake.read_parquet(Bucket.ARXIV_CLEAN, "arxiv/")
    dois = set(df["arxiv_doi"].dropna()) | set(df["journal_doi"].dropna())
    return sorted(dois)


def run(settings: Settings | None = None, workers: int = 4, chunk_size: int = 20_000) -> int:
    settings = settings or Settings.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="citations", layer=Layer.BRONZE, source="openalex",
                        task="ingest_citations_bronze") as metrics:
        dois = target_dois(lake)
        session = make_session()
        run_at = datetime.now(UTC)
        # 4 threads ≈ 8 req/s : sous la limite de 10 req/s du polite pool
        with ThreadPoolExecutor(max_workers=workers) as pool:
            works = [w for batch in pool.map(lambda b: fetch_batch(session, b, settings),
                                             batches(dois, DOIS_PER_REQUEST))
                     for w in batch]
        written = sum(
            lake.put_records(Bucket.CITATIONS_RAW,
                             partition_key("openalex", run_at, part, "jsonl"),
                             works[start:start + chunk_size])
            for part, start in enumerate(range(0, len(works), chunk_size))
        )
        metrics.records_in = len(dois)
        metrics.records_out = written
        metrics.extra = {"dois_queried": len(dois), "works_found": len(works)}
    return written
