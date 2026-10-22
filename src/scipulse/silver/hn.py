"""Silver Hacker News : hn-raw/ (enveloppes Firebase + Algolia) → hn-clean/ (Parquet).

  - deux schémas sources unifiés en un seul (id, score, commentaires, date...) ;
  - snapshots multiples d'une même story → on garde le plus récent (score à jour) ;
  - identifiant ArXiv extrait de l'URL (clé de jointure avec arxiv-clean) ;
  - valeurs manquantes : score / commentaires absents → 0 (story sans vote).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from scipulse.config import Bucket, Settings
from scipulse.monitoring import Layer, MetricsRecorder
from scipulse.silver.arxiv import clean_text
from scipulse.silver.gate import publish
from scipulse.silver.keys import arxiv_id_from_url
from scipulse.storage import DataLake


def _from_firebase(p: dict[str, Any]) -> dict[str, Any]:
    return {"hn_id": p.get("id"), "title": p.get("title"), "url": p.get("url"),
            "score": p.get("score"), "num_comments": p.get("descendants"),
            "author": p.get("by"), "created_ts": p.get("time")}


def _from_algolia(p: dict[str, Any]) -> dict[str, Any]:
    return {"hn_id": p.get("objectID"), "title": p.get("title"), "url": p.get("url"),
            "score": p.get("points"), "num_comments": p.get("num_comments"),
            "author": p.get("author"), "created_ts": p.get("created_at_i")}


PARSERS = {"firebase": _from_firebase, "algolia": _from_algolia}


def clean_envelope(envelope: dict[str, Any]) -> dict[str, Any] | None:
    parser = PARSERS.get(envelope.get("_source", ""))
    payload = envelope.get("payload")
    if parser is None or not isinstance(payload, dict):
        return None
    row = parser(payload)
    try:
        row["hn_id"] = int(row["hn_id"])
    except (TypeError, ValueError):
        return None
    row["title"] = clean_text(row["title"])
    row["source"] = envelope["_source"]
    row["ingested_at"] = envelope.get("_ingested_at")
    return row


def build_silver(envelopes: list[dict[str, Any]]) -> tuple[pd.DataFrame, int]:
    rows = [row for row in map(clean_envelope, envelopes) if row]
    rejected = len(envelopes) - len(rows)
    df = pd.DataFrame(rows)
    if df.empty:
        return df, rejected

    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, errors="coerce")
    df = (df.sort_values("ingested_at", na_position="first")
            .drop_duplicates("hn_id", keep="last"))
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).clip(lower=0).astype(int)
    df["num_comments"] = (pd.to_numeric(df["num_comments"], errors="coerce")
                          .fillna(0).clip(lower=0).astype(int))
    df["created_at"] = pd.to_datetime(df.pop("created_ts"), unit="s", utc=True, errors="coerce")
    df["created_year"] = df["created_at"].dt.year.astype("Int64")
    df["arxiv_id"] = df["url"].map(arxiv_id_from_url)
    return df.sort_values("hn_id").reset_index(drop=True), rejected


def run(settings: Settings | None = None) -> int:
    settings = settings or Settings.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="hn", layer=Layer.SILVER, source="hn",
                        task="clean_hn_silver") as metrics:
        envelopes = list(lake.iter_records(Bucket.HN_RAW))
        df, rejected = build_silver(envelopes)
        metrics.records_in = len(envelopes)
        metrics.records_rejected = rejected
        if not df.empty:
            metrics.data_watermark = df["created_at"].max().isoformat()
            metrics.extra = {"stories_with_arxiv_link": int(df["arxiv_id"].notna().sum())}
        publish(df, lake=lake, bucket=Bucket.HN_CLEAN, prefix="hn",
                suite="hn_silver", metrics=metrics)
    return metrics.records_out
