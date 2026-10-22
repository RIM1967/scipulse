"""SparkSession SciPulse : cluster standalone, MinIO via s3a, Elasticsearch via es-hadoop."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from scipulse.config import Settings

PACKAGE_DIR = Path(__file__).resolve().parents[1]   # src/scipulse


def package_code() -> str:
    """Zippe le package `scipulse` pour l'envoyer aux executors (UDF Python)."""
    target = Path(tempfile.gettempdir()) / "scipulse_pkg"
    return shutil.make_archive(str(target), "zip", root_dir=PACKAGE_DIR.parent,
                               base_dir=PACKAGE_DIR.name)


def build_session(settings: Settings, app_name: str = "scipulse-gold") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master_url)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", "1536m")
        .config("spark.sql.session.timeZone", "UTC")
        # AQE : ajuste partitions et stratégies de jointure à l'exécution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")   # 100 k lignes : 200 par défaut = trop
        # MinIO (API S3) via le connecteur hadoop-aws
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Elasticsearch via elasticsearch-hadoop (réseau Docker = pas de découverte de nœuds)
        .config("spark.es.nodes", settings.es_host.split("://")[-1].split(":")[0])
        .config("spark.es.port", settings.es_host.rsplit(":", 1)[-1])
        .config("spark.es.nodes.wan.only", "true")
    )
    if settings.spark_driver_host:
        builder = (builder.config("spark.driver.host", settings.spark_driver_host)
                          .config("spark.driver.bindAddress", "0.0.0.0"))
    spark = builder.getOrCreate()
    spark.sparkContext.addPyFile(package_code())
    spark.sparkContext.setLogLevel("WARN")
    return spark
