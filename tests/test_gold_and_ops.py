"""Tests : formule du score d'impact, alertes, dashboards Kibana générés, documents ES."""

from __future__ import annotations

import json
import math
from pathlib import Path

import kibana_objects
import pandas as pd
import pytest

from scipulse.alerts import build_alert
from scipulse.gold.es_index import load_mapping, to_document
from scipulse.monitoring import Layer, Status

ROOT = Path(__file__).resolve().parent.parent


# ── Score d'impact (référence Python pure) ───────────────────────────────────

@pytest.fixture(scope="module")
def scoring():
    """Le module importe pyspark : tests sautés si pyspark n'est pas installé."""
    return pytest.importorskip("scipulse.gold.scoring")


def test_recency_demi_vie_deux_ans(scoring) -> None:
    assert scoring.recency(0) == 1.0
    assert scoring.recency(2) == pytest.approx(0.5)
    assert scoring.recency(4) == pytest.approx(0.25)


def test_hn_signal_log_scale(scoring) -> None:
    assert scoring.hn_signal(0, 0, 1000) == 0.0
    assert scoring.hn_signal(1000, 0, 1000) == pytest.approx(1.0)
    # le log écrase les extrêmes : 10 % du max brut vaut bien plus que 10 % du signal
    assert scoring.hn_signal(100, 0, 1000) > 0.6


def test_impact_bornes_et_redistribution_sans_pagerank(scoring) -> None:
    assert scoring.impact(1, 1, 1, 1) == 100.0
    assert scoring.impact(0, 0, 0, 0) == 0.0
    # sans PageRank, les 3 poids restants sont renormalisés : le maximum reste 100
    assert scoring.impact(1, 1, 1, None) == pytest.approx(100.0)
    weights = scoring.Weights()
    assert math.isclose(weights.citations + weights.hn + weights.recency + weights.pagerank, 1)


# ── Alertes ──────────────────────────────────────────────────────────────────

def test_build_alert_qualifie_les_echecs_qualite() -> None:
    alert = build_alert("scipulse_arxiv_pipeline", "clean_hn_silver",
                        "QualityGateError('hn_silver : échec')")
    assert alert.status == Status.QUALITY_FAILED and alert.layer == Layer.SILVER
    assert alert.extra == {"alert": True}
    assert build_alert("d", "spark_gold_enrichment", "boom").layer == Layer.GOLD


# ── Documents Elasticsearch ──────────────────────────────────────────────────

def test_to_document_omet_les_valeurs_manquantes() -> None:
    row = {"a": 1, "b": float("nan"), "c": pd.NA, "d": pd.NaT,
           "e": pd.Timestamp("2024-01-02", tz="UTC"), "f": None}
    assert to_document(row) == {"a": 1, "e": "2024-01-02T00:00:00+00:00"}


def test_mapping_gold_couvre_les_colonnes_spark() -> None:
    spark_job = pytest.importorskip("scipulse.gold.spark_job")
    fields = set(load_mapping("arxiv-papers-enriched")["mappings"]["properties"])
    assert set(spark_job.GOLD_COLUMNS) == fields


# ── Dashboards Kibana générés ────────────────────────────────────────────────

def test_kibana_references_coherentes() -> None:
    objects = kibana_objects.build_objects()
    ids = {(o["type"], o["id"]) for o in objects}
    assert len(ids) == len(objects), "identifiants dupliqués"
    for obj in objects:
        for ref in obj["references"]:
            assert (ref["type"], ref["id"]) in ids, f"{obj['id']} → {ref['id']} introuvable"


def test_kibana_ndjson_versionne_a_jour() -> None:
    """Le .ndjson commité doit correspondre au générateur (sinon : relancer build)."""
    committed = (ROOT / "kibana" / "scipulse-dashboards.ndjson").read_text(encoding="utf-8")
    generated = [json.dumps(o, ensure_ascii=False) for o in kibana_objects.build_objects()]
    assert committed.splitlines() == generated


def test_trois_dashboards() -> None:
    titles = [o["attributes"]["title"] for o in kibana_objects.build_objects()
              if o["type"] == "dashboard"]
    assert len(titles) == 3
