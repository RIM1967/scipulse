"""Configuration centralisée, lue depuis les variables d'environnement.

Un seul point d'entrée pour tous les paramètres : aucun module ne lit
`os.environ` directement, ce qui rend le code testable (on injecte un
`Settings` construit à la main) et évite les valeurs dupliquées.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class Bucket(StrEnum):
    """Buckets MinIO de l'architecture Medallion (créés par scripts/init_buckets.py)."""

    # Bronze : JSON Lines brut, fidèle à la source
    ARXIV_RAW = "arxiv-raw"
    HN_RAW = "hn-raw"
    CITATIONS_RAW = "citations-raw"
    # Silver : Parquet nettoyé et validé, une source par bucket
    ARXIV_CLEAN = "arxiv-clean"
    HN_CLEAN = "hn-clean"
    CITATIONS_CLEAN = "citations-clean"
    # Data Docs Great Expectations (lecture publique)
    GX_DATA_DOCS = "gx-data-docs"


class EsIndex(StrEnum):
    """Index Elasticsearch (couche Gold + monitoring)."""

    ARXIV = "arxiv-papers"                      # Gold partiel (Module 1)
    ARXIV_ENRICHED = "arxiv-papers-enriched"    # Gold final (Module 2, Spark)
    METRICS = "scipulse-pipeline-metrics"       # Monitoring (alimenté par Logstash)


@dataclass(frozen=True)
class Settings:
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    es_host: str = "http://localhost:9200"
    spark_master_url: str = "local[*]"
    spark_driver_host: str = ""
    spark_driver_memory: str = "1g"
    openalex_mailto: str = ""
    openalex_api_key: str = ""
    alert_webhook_url: str = ""
    metrics_dir: Path = Path("data/metrics")
    data_dir: Path = Path("data")

    @classmethod
    def from_env(cls) -> Settings:
        """Construit la configuration depuis l'environnement (valeurs par défaut sinon)."""
        env = os.environ.get
        defaults = cls()
        return cls(
            s3_endpoint_url=env("S3_ENDPOINT_URL", defaults.s3_endpoint_url),
            s3_access_key=env("S3_ACCESS_KEY", defaults.s3_access_key),
            s3_secret_key=env("S3_SECRET_KEY", defaults.s3_secret_key),
            es_host=env("ES_HOST", defaults.es_host),
            spark_master_url=env("SPARK_MASTER_URL", defaults.spark_master_url),
            spark_driver_host=env("SPARK_DRIVER_HOST", defaults.spark_driver_host),
            spark_driver_memory=env("SPARK_DRIVER_MEMORY", defaults.spark_driver_memory),
            openalex_mailto=env("OPENALEX_MAILTO", defaults.openalex_mailto),
            openalex_api_key=env("OPENALEX_API_KEY", defaults.openalex_api_key),
            alert_webhook_url=env("ALERT_WEBHOOK_URL", defaults.alert_webhook_url),
            metrics_dir=Path(env("METRICS_DIR", str(defaults.metrics_dir))),
            data_dir=Path(env("DATA_DIR", str(defaults.data_dir))),
        )
