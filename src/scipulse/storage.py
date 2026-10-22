"""Accès au data lake MinIO (API S3) : écriture/lecture Bronze (JSON Lines) et Silver (Parquet).

Toutes les couches passent par cette classe : un seul endroit gère les
détails S3 (pagination, encodage, sérialisation Parquet).
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Any

import boto3
from botocore.client import Config

from scipulse.config import Settings

if TYPE_CHECKING:
    import pandas as pd


def make_s3_client(settings: Settings) -> Any:
    """Client boto3 pointant sur MinIO (signature v4, région factice obligatoire)."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def partition_key(prefix: str, run_at: datetime, part: int, ext: str) -> str:
    """Clé partitionnée par date d'ingestion, au format Hive (lisible par Spark).

    >>> partition_key("arxiv", datetime(2026, 10, 22, 8, 5), 3, "jsonl")
    'arxiv/ingest_date=2026-10-22/part-20261022T080500-0003.jsonl'

    L'horodatage dans le nom évite d'écraser les fichiers d'un run précédent
    le même jour (idempotence par run, historique conservé en Bronze).
    """
    return (f"{prefix}/ingest_date={run_at:%Y-%m-%d}/"
            f"part-{run_at:%Y%m%dT%H%M%S}-{part:04d}.{ext}")


class DataLake:
    """Opérations haut niveau sur les buckets Medallion."""

    def __init__(self, client: Any) -> None:
        self.client = client

    @classmethod
    def from_settings(cls, settings: Settings) -> DataLake:
        return cls(make_s3_client(settings))

    # ── Écriture ─────────────────────────────────────────────────────────────

    def put_lines(self, bucket: str, key: str, lines: Iterable[str]) -> int:
        """Écrit des lignes déjà sérialisées, *octet pour octet* (Bronze fidèle à la source).

        Renvoie le nombre de lignes écrites.
        """
        buffer = io.BytesIO()
        count = 0
        for line in lines:
            buffer.write(line.rstrip("\n").encode("utf-8") + b"\n")
            count += 1
        self.client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue(),
                               ContentType="application/x-ndjson")
        return count

    def put_records(self, bucket: str, key: str, records: Iterable[dict[str, Any]]) -> int:
        """Sérialise des dictionnaires (réponses d'API) en JSON Lines."""
        return self.put_lines(bucket, key, (json.dumps(r, ensure_ascii=False) for r in records))

    def put_parquet(self, bucket: str, key: str, df: pd.DataFrame) -> int:
        """Écrit un DataFrame pandas en Parquet (compression snappy). Renvoie le nb de lignes."""
        buffer = io.BytesIO()
        # Horodatages en microsecondes : Spark 3.5 ne lit pas les TIMESTAMP(NANOS)
        # que pandas écrit par défaut
        df.to_parquet(buffer, index=False, compression="snappy",
                      coerce_timestamps="us", allow_truncated_timestamps=True)
        self.client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        return len(df)

    # ── Lecture ──────────────────────────────────────────────────────────────

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        """Liste toutes les clés sous un préfixe (gère la pagination S3 par 1000)."""
        paginator = self.client.get_paginator("list_objects_v2")
        return [obj["Key"]
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
                for obj in page.get("Contents", [])]

    def iter_records(self, bucket: str, prefix: str = "") -> Iterator[dict[str, Any]]:
        """Itère sur les enregistrements JSON Lines de tous les fichiers d'un préfixe.

        Les lignes non décodables sont ignorées ici : leur comptage relève de
        l'analyse qualité (Silver), pas de la lecture.
        """
        for key in self.list_keys(bucket, prefix):
            body = self.client.get_object(Bucket=bucket, Key=key)["Body"]
            for raw in body.iter_lines():
                if raw.strip():
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError:
                        continue

    def read_parquet(self, bucket: str, prefix: str = "") -> pd.DataFrame:
        """Concatène tous les fichiers Parquet d'un préfixe en un DataFrame."""
        import pandas as pd

        frames = [
            pd.read_parquet(io.BytesIO(self.client.get_object(Bucket=bucket, Key=k)["Body"].read()))
            for k in self.list_keys(bucket, prefix) if k.endswith(".parquet")
        ]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """Supprime tous les objets d'un préfixe (réécriture idempotente d'une partition Silver)."""
        keys = self.list_keys(bucket, prefix)
        for start in range(0, len(keys), 1000):
            self.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in keys[start:start + 1000]]},
            )
        return len(keys)
