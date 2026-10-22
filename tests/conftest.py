"""Fixtures partagées : data lake S3 simulé en mémoire (moto), sans MinIO réel."""

from __future__ import annotations

from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws

from scipulse.config import Bucket
from scipulse.storage import DataLake


@pytest.fixture
def lake() -> Iterator[DataLake]:
    """DataLake branché sur un S3 simulé, avec tous les buckets Medallion créés."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1",
                              aws_access_key_id="test", aws_secret_access_key="test")
        for bucket in Bucket:
            client.create_bucket(Bucket=bucket.value)
        yield DataLake(client)
