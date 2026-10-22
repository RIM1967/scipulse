"""Gestion des index Elasticsearch de la couche Gold.

Les mappings sont versionnés dans `elasticsearch/*.json` : les types sont
explicites (`dynamic: strict`), jamais devinés par ES. Un champ inattendu
fait échouer l'indexation au lieu de polluer silencieusement le mapping.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scipulse.config import Bucket, EsIndex, Settings
from scipulse.monitoring import Layer, MetricsRecorder
from scipulse.storage import DataLake

if TYPE_CHECKING:
    import pandas as pd
    from elasticsearch import Elasticsearch

MAPPINGS_DIR = Path(__file__).resolve().parents[3] / "elasticsearch"


def load_mapping(index: str) -> dict[str, Any]:
    return json.loads((MAPPINGS_DIR / f"{index}.json").read_text(encoding="utf-8"))


def recreate_index(es: Elasticsearch, index: str) -> None:
    """Supprime puis recrée l'index avec son mapping versionné (rechargement complet)."""
    es.options(ignore_status=404).indices.delete(index=index)
    body = load_mapping(index)
    es.indices.create(index=index, settings=body["settings"], mappings=body["mappings"])


def to_document(row: dict[str, Any]) -> dict[str, Any]:
    """Ligne pandas → document JSON (NaN/NaT → absent, dates → ISO 8601, numpy → natif)."""
    doc: dict[str, Any] = {}
    for key, value in row.items():
        if hasattr(value, "tolist"):                       # numpy array / scalaire
            value = value.tolist()
        # None, NaN et pd.NA (colonnes Int64 nullables) : champ omis
        if (value is None or type(value).__name__ == "NAType"
                or (isinstance(value, float) and math.isnan(value))):
            continue
        if hasattr(value, "isoformat"):
            if str(value) == "NaT":
                continue
            value = value.isoformat()
        doc[key] = value
    return doc


def iter_actions(df: pd.DataFrame, index: str, id_field: str) -> Iterator[dict[str, Any]]:
    for row in df.to_dict(orient="records"):
        doc = to_document(row)
        yield {"_index": index, "_id": doc[id_field], "_source": doc}


def index_arxiv(settings: Settings | None = None) -> int:
    """Module 1 — Gold partiel : Silver ArXiv → index `arxiv-papers`."""
    from elasticsearch import Elasticsearch, helpers

    settings = settings or Settings.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="arxiv", layer=Layer.GOLD, source="arxiv",
                        task="index_arxiv_gold") as metrics:
        df = lake.read_parquet(Bucket.ARXIV_CLEAN, "arxiv/")
        es = Elasticsearch(settings.es_host, request_timeout=120)
        recreate_index(es, EsIndex.ARXIV)
        indexed, errors = helpers.bulk(es, iter_actions(df, EsIndex.ARXIV, "arxiv_id"),
                                       chunk_size=2_000, raise_on_error=False)
        es.indices.refresh(index=EsIndex.ARXIV)
        metrics.records_in = len(df)
        metrics.records_out = indexed
        metrics.records_rejected = len(errors) if isinstance(errors, list) else int(errors)
    return indexed
