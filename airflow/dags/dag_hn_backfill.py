"""SciPulse — DAG de backfill Hacker News : historique des liens arxiv.org (API Algolia).

À déclencher une fois (manuellement) : sans cet historique, le recouvrement
ArXiv × HN serait quasi nul, le poller ne voyant que les stories récentes.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse.alerts import on_failure


def backfill_hn_algolia() -> int:
    from scipulse.bronze import hn

    return hn.backfill()


with DAG(
    dag_id="scipulse_hn_backfill",
    description="Hacker News : backfill des stories pointant vers arxiv.org (Algolia)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=2),
                  "on_failure_callback": on_failure},
    tags=["scipulse", "hacker-news", "backfill"],
) as dag:

    PythonOperator(
        task_id="backfill_hn_algolia",
        python_callable=backfill_hn_algolia,
        execution_timeout=timedelta(minutes=30),
    )
