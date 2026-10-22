"""Tests du socle commun : configuration, data lake, métriques."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from scipulse.config import Bucket, Settings
from scipulse.monitoring import Layer, MetricsRecorder, Status
from scipulse.storage import DataLake, partition_key

# ── config ───────────────────────────────────────────────────────────────────

def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("METRICS_DIR", "/tmp/m")
    settings = Settings.from_env()
    assert settings.s3_endpoint_url == "http://minio:9000"
    assert settings.metrics_dir == Path("/tmp/m")


def test_settings_valeurs_par_defaut(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ES_HOST", raising=False)
    assert Settings.from_env().es_host == "http://localhost:9200"


# ── storage ──────────────────────────────────────────────────────────────────

def test_partition_key_format_hive() -> None:
    key = partition_key("hn", datetime(2026, 1, 2, 3, 4, 5), 7, "jsonl")
    assert key == "hn/ingest_date=2026-01-02/part-20260102T030405-0007.jsonl"


def test_put_lines_conserve_les_octets_source(lake: DataLake) -> None:
    raw = ['{"id": "1", "title": "é"}\n', '{"id":"2"}']
    assert lake.put_lines(Bucket.ARXIV_RAW, "a/x.jsonl", raw) == 2
    body = lake.client.get_object(Bucket=Bucket.ARXIV_RAW, Key="a/x.jsonl")["Body"].read()
    assert body == '{"id": "1", "title": "é"}\n{"id":"2"}\n'.encode()


def test_iter_records_ignore_les_lignes_invalides(lake: DataLake) -> None:
    lake.put_lines(Bucket.HN_RAW, "p/1.jsonl", ['{"a": 1}', "pas du json", '{"a": 2}'])
    lake.put_records(Bucket.HN_RAW, "p/2.jsonl", [{"a": 3}])
    assert [r["a"] for r in lake.iter_records(Bucket.HN_RAW, "p/")] == [1, 2, 3]


def test_parquet_aller_retour(lake: DataLake) -> None:
    df = pd.DataFrame({"id": ["a", "b"], "score": [1, 2]})
    lake.put_parquet(Bucket.HN_CLEAN, "t/part-0.parquet", df)
    pd.testing.assert_frame_equal(lake.read_parquet(Bucket.HN_CLEAN, "t/"), df)


def test_read_parquet_prefixe_vide(lake: DataLake) -> None:
    assert lake.read_parquet(Bucket.HN_CLEAN, "absent/").empty


def test_delete_prefix(lake: DataLake) -> None:
    for i in range(3):
        lake.put_lines(Bucket.ARXIV_CLEAN, f"d/{i}.jsonl", ["{}"])
    lake.put_lines(Bucket.ARXIV_CLEAN, "garde/0.jsonl", ["{}"])
    assert lake.delete_prefix(Bucket.ARXIV_CLEAN, "d/") == 3
    assert lake.list_keys(Bucket.ARXIV_CLEAN) == ["garde/0.jsonl"]


# ── monitoring ───────────────────────────────────────────────────────────────

def _read_metrics(directory: Path) -> list[dict]:
    return [json.loads(line)
            for path in directory.glob("*.jsonl")
            for line in path.read_text(encoding="utf-8").splitlines()]


def test_track_enregistre_un_succes(tmp_path: Path) -> None:
    recorder = MetricsRecorder(tmp_path)
    with recorder.track(pipeline="arxiv", layer=Layer.BRONZE, source="arxiv",
                        task="ingest") as m:
        m.records_out = 42
    [row] = _read_metrics(tmp_path)
    assert row["status"] == "success" and row["records_out"] == 42
    assert row["layer"] == "bronze" and row["duration_s"] >= 0


def test_track_enregistre_l_echec_et_propage(tmp_path: Path) -> None:
    recorder = MetricsRecorder(tmp_path)
    with pytest.raises(ValueError), recorder.track(
        pipeline="hn", layer=Layer.SILVER, source="hn", task="clean"
    ):
        raise ValueError("boom")
    [row] = _read_metrics(tmp_path)
    assert row["status"] == "failed" and "boom" in row["error"]


def test_track_conserve_le_statut_qualite(tmp_path: Path) -> None:
    recorder = MetricsRecorder(tmp_path)
    with pytest.raises(RuntimeError), recorder.track(
        pipeline="hn", layer=Layer.SILVER, source="hn", task="validate"
    ) as m:
        m.status = Status.QUALITY_FAILED
        raise RuntimeError("suite GX en échec")
    assert _read_metrics(tmp_path)[0]["status"] == "quality_failed"
