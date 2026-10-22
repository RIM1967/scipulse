"""Alertes d'échec du pipeline (callback Airflow `on_failure_callback`).

Deux canaux :
  - une métrique `alert=true` dans l'index de monitoring : visible
    immédiatement dans le dashboard Kibana (panneau « Alertes ») ;
  - un webhook optionnel (ALERT_WEBHOOK_URL : Slack, Teams, Discord...).
Un échec d'alerte ne doit jamais masquer l'erreur d'origine : tout est
protégé par try/except.
"""

from __future__ import annotations

import logging
from typing import Any

from scipulse.config import Settings
from scipulse.monitoring import Layer, MetricsRecorder, RunMetrics, Status

log = logging.getLogger("scipulse.alerts")

LAYER_BY_PREFIX = {"ingest": Layer.BRONZE, "poll": Layer.BRONZE, "backfill": Layer.BRONZE,
                   "clean": Layer.SILVER}


def build_alert(dag_id: str, task_id: str, error: str) -> RunMetrics:
    quality = "QualityGateError" in error
    layer = LAYER_BY_PREFIX.get(task_id.split("_", 1)[0], Layer.GOLD)
    return RunMetrics(
        pipeline=dag_id, layer=layer, source=task_id.rsplit("_", 1)[0], task=task_id,
        status=Status.QUALITY_FAILED if quality else Status.FAILED,
        error=error[:500], extra={"alert": True},
    )


def on_failure(context: dict[str, Any]) -> None:
    """Callback Airflow : appelé après l'échec définitif d'une tâche (retries épuisés)."""
    settings = Settings.from_env()
    task = context["task_instance"]
    error = repr(context.get("exception"))
    alert = build_alert(task.dag_id, task.task_id, error)
    try:
        MetricsRecorder(settings.metrics_dir).record(alert)
    except OSError:
        log.exception("Impossible d'écrire la métrique d'alerte")

    if settings.alert_webhook_url:
        try:
            import requests

            requests.post(settings.alert_webhook_url, timeout=10, json={
                "text": f":rotating_light: SciPulse — échec {task.dag_id}.{task.task_id} "
                        f"({alert.status}) : {error[:300]}\nLogs : {task.log_url}",
            })
        except Exception:  # noqa: BLE001
            log.exception("Webhook d'alerte injoignable")
