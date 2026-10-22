"""Crée les buckets du data lake Medallion dans MinIO (service one-shot `minio-init`).

Remplace le conteneur `minio/mc`, qui n'est plus distribué publiquement.
Idempotent : les buckets existants sont conservés.
"""

from __future__ import annotations

import json
import os
import sys
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# Bronze (JSON Lines brut) → Silver (Parquet nettoyé) ; Gold = Elasticsearch
BUCKETS = [
    "arxiv-raw", "hn-raw", "citations-raw",
    "arxiv-clean", "hn-clean", "citations-clean",
    "gx-data-docs",
]
# Bucket servi en lecture anonyme : Data Docs GX consultables dans le navigateur
PUBLIC_BUCKETS = {"gx-data-docs"}


def public_read_policy(bucket: str) -> str:
    """Politique S3 autorisant uniquement la lecture anonyme des objets."""
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{bucket}/*"],
        }],
    })


def main() -> int:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # MinIO peut mettre quelques secondes à accepter les requêtes
    for attempt in range(30):
        try:
            existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
            break
        except (EndpointConnectionError, ClientError) as exc:
            print(f"MinIO pas prêt ({attempt + 1}/30) : {exc}")
            time.sleep(2)
    else:
        return 1

    for bucket in BUCKETS:
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
        if bucket in PUBLIC_BUCKETS:
            s3.put_bucket_policy(Bucket=bucket, Policy=public_read_policy(bucket))
        print(f"bucket ok : {bucket}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
