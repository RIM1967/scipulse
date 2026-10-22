"""Silver ArXiv : arxiv-raw/ (JSON Lines) → arxiv-clean/ (Parquet validé).

Nettoyage guidé par l'analyse qualité Bronze (notebook 01) :
  - doublons d'identifiant (re-ingestions)        → on garde la mise à jour la plus récente ;
  - identifiant / titre manquants ou invalides    → lignes rejetées (comptées) ;
  - espaces et retours à la ligne dans les textes → normalisés ;
  - dates RFC 2822 hétérogènes                    → horodatages UTC ;
  - catégories en chaîne « cs.LG stat.ML »        → liste + catégorie primaire
    + domaine + libellé lisible (encodage des catégories) ;
  - auteurs                                        → liste « Prénom Nom » + nombre ;
  - DOI éditeur                                    → normalisé ; DOI ArXiv ajouté
    (clé de jointure avec OpenAlex).
"""

from __future__ import annotations

import re
from email.utils import parsedate_to_datetime
from typing import Any

import pandas as pd

from scipulse.config import Bucket, Settings
from scipulse.monitoring import Layer, MetricsRecorder
from scipulse.silver.gate import publish
from scipulse.silver.keys import arxiv_doi, normalize_arxiv_id, normalize_doi
from scipulse.storage import DataLake

# Taxonomie officielle ArXiv (informatique) : https://arxiv.org/category_taxonomy
CS_CATEGORIES = {
    "cs.AI": "Artificial Intelligence", "cs.AR": "Hardware Architecture",
    "cs.CC": "Computational Complexity", "cs.CE": "Computational Engineering",
    "cs.CG": "Computational Geometry", "cs.CL": "Computation and Language",
    "cs.CR": "Cryptography and Security", "cs.CV": "Computer Vision",
    "cs.CY": "Computers and Society", "cs.DB": "Databases",
    "cs.DC": "Distributed Computing", "cs.DL": "Digital Libraries",
    "cs.DM": "Discrete Mathematics", "cs.DS": "Data Structures and Algorithms",
    "cs.ET": "Emerging Technologies", "cs.FL": "Formal Languages",
    "cs.GL": "General Literature", "cs.GR": "Graphics",
    "cs.GT": "Game Theory", "cs.HC": "Human-Computer Interaction",
    "cs.IR": "Information Retrieval", "cs.IT": "Information Theory",
    "cs.LG": "Machine Learning", "cs.LO": "Logic in Computer Science",
    "cs.MA": "Multiagent Systems", "cs.MM": "Multimedia",
    "cs.MS": "Mathematical Software", "cs.NA": "Numerical Analysis",
    "cs.NE": "Neural and Evolutionary Computing", "cs.NI": "Networking",
    "cs.OH": "Other Computer Science", "cs.OS": "Operating Systems",
    "cs.PF": "Performance", "cs.PL": "Programming Languages",
    "cs.RO": "Robotics", "cs.SC": "Symbolic Computation",
    "cs.SD": "Sound", "cs.SE": "Software Engineering",
    "cs.SI": "Social and Information Networks", "cs.SY": "Systems and Control",
}

_SPACES = re.compile(r"\s+")


def clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _SPACES.sub(" ", value).strip()
    return text or None


def parse_rfc2822(value: Any) -> pd.Timestamp | None:
    try:
        return pd.Timestamp(parsedate_to_datetime(value)).tz_convert("UTC")
    except (TypeError, ValueError, IndexError):
        return None


def parse_authors(authors_parsed: Any) -> list[str]:
    """[['Vaswani', 'Ashish', ''], ...] → ['Ashish Vaswani', ...]"""
    if not isinstance(authors_parsed, list):
        return []
    names = []
    for parts in authors_parsed:
        if isinstance(parts, list) and parts:
            last, first = parts[0], parts[1] if len(parts) > 1 else ""
            name = clean_text(f"{first} {last}")
            if name:
                names.append(name)
    return names


def clean_record(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Transforme un enregistrement Bronze en ligne Silver, ou None s'il est inexploitable."""
    arxiv_id = normalize_arxiv_id(raw.get("id"))
    title = clean_text(raw.get("title"))
    if not arxiv_id or not title:
        return None

    categories = (raw.get("categories") or "").split()
    primary = categories[0] if categories else None
    versions = raw.get("versions") or []
    published_at = parse_rfc2822(versions[0].get("created")) if versions else None
    updated_raw = raw.get("update_date")
    authors = parse_authors(raw.get("authors_parsed"))

    return {
        "arxiv_id": arxiv_id,
        "title": title,
        "abstract": clean_text(raw.get("abstract")),
        "authors": authors,
        "n_authors": len(authors),
        "submitter": clean_text(raw.get("submitter")),
        "categories": categories,
        "primary_category": primary,
        "primary_domain": primary.split(".")[0] if primary else None,
        "category_name": CS_CATEGORIES.get(primary or "", primary),
        "published_at": published_at,
        "published_year": published_at.year if published_at is not None else None,
        "updated_at": pd.to_datetime(updated_raw, utc=True, errors="coerce"),
        "n_versions": len(versions),
        "journal_ref": clean_text(raw.get("journal-ref")),
        "journal_doi": normalize_doi(raw.get("doi")),
        "arxiv_doi": arxiv_doi(arxiv_id),
        "license": clean_text(raw.get("license")),
    }


def build_silver(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, int]:
    """Nettoie et déduplique. Renvoie (DataFrame Silver, nb de lignes rejetées)."""
    rows = [row for row in map(clean_record, records) if row]
    rejected = len(records) - len(rows)
    df = pd.DataFrame(rows)
    if df.empty:
        return df, rejected
    df = (df.sort_values("updated_at", na_position="first")
            .drop_duplicates("arxiv_id", keep="last")
            .sort_values("arxiv_id")
            .reset_index(drop=True))
    df["published_year"] = df["published_year"].astype("Int64")
    return df, rejected


def run(settings: Settings | None = None) -> int:
    settings = settings or Settings.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="arxiv", layer=Layer.SILVER, source="arxiv",
                        task="clean_arxiv_silver") as metrics:
        records = list(lake.iter_records(Bucket.ARXIV_RAW, "arxiv/"))
        df, rejected = build_silver(records)
        metrics.records_in = len(records)
        metrics.records_rejected = rejected
        metrics.extra = {"duplicates_removed": len(records) - rejected - len(df)}
        if not df.empty:
            metrics.data_watermark = df["published_at"].max().isoformat()
        publish(df, lake=lake, bucket=Bucket.ARXIV_CLEAN, prefix="arxiv",
                suite="arxiv_silver", metrics=metrics)
    return metrics.records_out
