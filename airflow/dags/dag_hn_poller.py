"""SciPulse — DAG micro-batch Hacker News (toutes les 5 minutes) → Bronze hn-raw/."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scipulse.alerts import on_failure


def poll_hn_bronze() -> int:
    from scipulse.bronze import hn

    return hn.poll()


with DAG(
    dag_id="scipulse_hn_poller",
    description="Hacker News : snapshot micro-batch des stories (API Firebase)",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,                 # pas de rattrapage : seul l'état courant a du sens
    max_active_runs=1,             # jamais deux polls simultanés
    dagrun_timeout=timedelta(minutes=4),
    default_args={
        "owner": "scipulse",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": on_failure,
    },
    tags=["scipulse", "hacker-news", "micro-batch"],
) as dag:

    PythonOperator(
        task_id="poll_hn_bronze",
        python_callable=poll_hn_bronze,
        execution_timeout=timedelta(minutes=3),
    )
