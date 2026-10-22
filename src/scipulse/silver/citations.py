"""Silver citations : citations-raw/ (works OpenAlex) → citations-clean/ (Parquet).

  - DOI normalisé (minuscules, sans https://doi.org/) : clé de jointure ;
  - identifiant ArXiv déduit quand le DOI est un DOI ArXiv (10.48550/arxiv.*) ;
  - identifiants OpenAlex raccourcis (W123...) pour le graphe de citations ;
  - `counts_by_year` conservé en liste de structures (vélocité dans Spark) ;
  - doublons (un même work renvoyé pour plusieurs DOI) supprimés.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from scipulse.config import Bucket, Settings
from scipulse.monitoring import Layer, MetricsRecorder
from scipulse.silver.arxiv import clean_text
from scipulse.silver.gate import publish
from scipulse.silver.keys import arxiv_id_from_doi, normalize_doi
from scipulse.storage import DataLake


def short_openalex_id(url: Any) -> str | None:
    """'https://openalex.org/W2741809807' → 'W2741809807'"""
    return url.rsplit("/", 1)[-1] if isinstance(url, str) and url else None


def clean_work(work: dict[str, Any]) -> dict[str, Any] | None:
    openalex_id = short_openalex_id(work.get("id"))
    doi = normalize_doi(work.get("doi"))
    if not openalex_id or not doi:
        return None
    topic = work.get("primary_topic") or {}
    return {
        "openalex_id": openalex_id,
        "doi": doi,
        "arxiv_id": arxiv_id_from_doi(doi),
        "oa_title": clean_text(work.get("title")),
        "oa_publication_year": work.get("publication_year"),
        "oa_type": work.get("type"),
        "cited_by_count": int(work.get("cited_by_count") or 0),
        "fwci": work.get("fwci"),
        "counts_by_year": [
            {"year": int(c["year"]), "cited_by_count": int(c.get("cited_by_count") or 0)}
            for c in work.get("counts_by_year") or [] if c.get("year")
        ],
        "referenced_works": [r for r in map(short_openalex_id, work.get("referenced_works") or [])
                             if r],
        "topic": (topic.get("display_name")),
        "topic_subfield": (topic.get("subfield") or {}).get("display_name"),
        "topic_field": (topic.get("field") or {}).get("display_name"),
        "topic_domain": (topic.get("domain") or {}).get("display_name"),
    }


def build_silver(works: list[dict[str, Any]]) -> tuple[pd.DataFrame, int]:
    rows = [row for row in map(clean_work, works) if row]
    rejected = len(works) - len(rows)
    df = pd.DataFrame(rows)
    if df.empty:
        return df, rejected
    df = df.drop_duplicates("openalex_id", keep="last").reset_index(drop=True)
    df["oa_publication_year"] = pd.to_numeric(df["oa_publication_year"],
                                              errors="coerce").astype("Int64")
    df["fwci"] = pd.to_numeric(df["fwci"], errors="coerce")
    return df, rejected


def run(settings: Settings | None = None) -> int:
    settings = settings or Settings.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="citations", layer=Layer.SILVER, source="openalex",
                        task="clean_citations_silver") as metrics:
        works = list(lake.iter_records(Bucket.CITATIONS_RAW, "openalex/"))
        df, rejected = build_silver(works)
        metrics.records_in = len(works)
        metrics.records_rejected = rejected
        publish(df, lake=lake, bucket=Bucket.CITATIONS_CLEAN, prefix="openalex",
                suite="citations_silver", metrics=metrics)
    return metrics.records_out
