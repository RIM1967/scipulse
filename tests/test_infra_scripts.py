"""Tests des scripts d'infrastructure (génération .env, sonde de santé)."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import check_stack
import init_env

ROOT = Path(__file__).resolve().parent.parent


# ── init_env ─────────────────────────────────────────────────────────────────

def test_render_env_remplace_tous_les_marqueurs() -> None:
    rendered = init_env.render_env((ROOT / ".env.example").read_text(encoding="utf-8"))
    assert "__GENERATE__" not in rendered
    assert "__FERNET__" not in rendered


def test_render_env_secrets_distincts_par_occurrence() -> None:
    rendered = init_env.render_env("A=__GENERATE__\nB=__GENERATE__")
    a, b = (line.split("=", 1)[1] for line in rendered.splitlines())
    assert a != b and len(a) >= 24


def test_render_env_cle_fernet_valide() -> None:
    key = init_env.render_env("K=__FERNET__").strip().split("=", 1)[1]
    assert len(base64.urlsafe_b64decode(key)) == 32


def test_env_example_couvre_les_variables_du_compose() -> None:
    """Chaque ${VAR} obligatoire du compose doit être déclarée dans .env.example."""
    import re

    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    used = set(re.findall(r"\$\{(\w+)(?::?[-?][^}]*)?\}", compose))
    declared = {
        line.split("=", 1)[0]
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.startswith("#")
    }
    assert used - declared == set()


# ── check_stack ──────────────────────────────────────────────────────────────

def test_es_status() -> None:
    assert check_stack.es_status(json.dumps({"status": "yellow"}))[0]
    assert not check_stack.es_status(json.dumps({"status": "red"}))[0]


def test_kibana_status() -> None:
    body = {"status": {"overall": {"level": "degraded"}}}
    assert not check_stack.kibana_status(json.dumps(body))[0]


def test_airflow_status() -> None:
    healthy = {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
    assert check_stack.airflow_status(json.dumps(healthy))[0]
    healthy["scheduler"]["status"] = "unhealthy"
    assert not check_stack.airflow_status(json.dumps(healthy))[0]


def test_spark_status_exige_un_worker() -> None:
    assert check_stack.spark_status(json.dumps({"aliveworkers": 1, "cores": 2}))[0]
    assert not check_stack.spark_status(json.dumps({"aliveworkers": 0}))[0]
