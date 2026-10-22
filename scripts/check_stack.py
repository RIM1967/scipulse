"""Vérifie que toute la stack SciPulse est opérationnelle depuis l'hôte.

Interroge chaque service sur son endpoint de santé et contrôle que les
conteneurs one-shot (initialisation) ont terminé sans erreur.
Uniquement la bibliothèque standard : exécutable sans environnement virtuel.

Usage : python scripts/check_stack.py [--wait SECONDES]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    name: str
    url: str
    # Reçoit le corps de la réponse, renvoie (ok, détail)
    validate: Callable[[str], tuple[bool, str]]


def es_status(body: str) -> tuple[bool, str]:
    status = json.loads(body)["status"]
    return status in {"green", "yellow"}, f"cluster {status}"


def kibana_status(body: str) -> tuple[bool, str]:
    level = json.loads(body)["status"]["overall"]["level"]
    return level == "available", level


def airflow_status(body: str) -> tuple[bool, str]:
    payload = json.loads(body)
    states = {name: payload[name]["status"] for name in ("metadatabase", "scheduler")}
    return all(s == "healthy" for s in states.values()), str(states)


def spark_status(body: str) -> tuple[bool, str]:
    payload = json.loads(body)
    workers = payload.get("aliveworkers", 0)
    return workers >= 1, f"{workers} worker(s), {payload.get('cores', 0)} cœurs"


def http_ok(_body: str) -> tuple[bool, str]:
    return True, "up"


CHECKS = [
    Check("Elasticsearch", "http://localhost:9200/_cluster/health", es_status),
    Check("Kibana", "http://localhost:5601/api/status", kibana_status),
    Check("Logstash", "http://localhost:9600", http_ok),
    Check("MinIO", "http://localhost:9000/minio/health/live", http_ok),
    Check("Airflow", "http://localhost:8080/health", airflow_status),
    Check("Spark master", "http://localhost:8088/json/", spark_status),
]

# Conteneurs d'initialisation qui doivent s'être terminés avec le code 0
ONE_SHOT = ["scipulse-airflow-init", "scipulse-minio-init"]


def run_check(check: Check) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(check.url, timeout=5) as response:
            return check.validate(response.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 — tout échec = service indisponible
        return False, type(exc).__name__


def one_shot_status(container: str) -> tuple[bool, str]:
    result = subprocess.run(
        ["docker", "inspect", container, "--format", "{{.State.Status}} {{.State.ExitCode}}"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        return False, "conteneur introuvable"
    state, code = result.stdout.split()
    return state == "exited" and code == "0", f"{state} (code {code})"


def run_all() -> list[tuple[str, bool, str]]:
    results = [(c.name, *run_check(c)) for c in CHECKS]
    results += [(name, *one_shot_status(name)) for name in ONE_SHOT]
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", type=int, default=0,
                        help="attendre jusqu'à N secondes que tout soit prêt")
    args = parser.parse_args()

    deadline = time.monotonic() + args.wait
    while True:
        results = run_all()
        all_ok = all(ok for _, ok, _ in results)
        if all_ok or time.monotonic() >= deadline:
            break
        time.sleep(10)

    for name, ok, detail in results:
        print(f"{'OK ' if ok else 'KO '} {name:<22} {detail}")
    print("\nStack opérationnelle." if all_ok else "\nCertains services ne sont pas prêts.")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
