"""SciPulse — DAG batch de bout en bout : Bronze → Silver → Gold.

    ingest_arxiv_bronze → clean_arxiv_silver ─┬→ index_arxiv_gold          (Module 1)
                                              └→ ingest_citations_bronze → clean_citations_silver ─┐
    clean_hn_silver ──────────────────────────────────────────────────────────────────────────┤
                                                                       spark_gold_enrichment ←─┘
                                                                                  ↓
                                                                         validate_gold

Les tâches appellent le code de `src/scipulse` : le DAG ne contient que
l'orchestration. Imports *dans* les callables pour garder le parsing rapide.
Un échec (dont un échec de suite GX) déclenche `on_failure` (alerte) et
bloque les tâches en aval : aucune donnée invalide n'atteint le Gold.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from scipulse.alerts import on_failure

SRC = "/opt/airflow/src"


def ingest_arxiv_bronze() -> int:
    from scipulse.bronze import arxiv
    return arxiv.run()


def clean_arxiv_silver() -> int:
    from scipulse.silver import arxiv
    return arxiv.run()


def clean_hn_silver() -> int:
    from scipulse.silver import hn
    return hn.run()


def index_arxiv_gold() -> int:
    from scipulse.gold.es_index import index_arxiv
    return index_arxiv()


def ingest_citations_bronze() -> int:
    from scipulse.bronze import openalex
    return openalex.run()


def clean_citations_silver() -> int:
    from scipulse.silver import citations
    return citations.run()


def validate_gold() -> dict:
    from scipulse.gold.validation import run_checks
    return run_checks()


with DAG(
    dag_id="scipulse_arxiv_pipeline",
    description="Pipeline Medallion complet : ArXiv + HN + OpenAlex → Gold Elasticsearch",
    schedule=None,                 # batch déclenché manuellement (dump statique)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "scipulse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": on_failure,
    },
    tags=["scipulse", "arxiv", "batch", "medallion"],
) as dag:

    def task(callable_, minutes: int = 30) -> PythonOperator:
        return PythonOperator(task_id=callable_.__name__, python_callable=callable_,
                              execution_timeout=timedelta(minutes=minutes))

    bronze_arxiv = task(ingest_arxiv_bronze, minutes=60)   # téléchargement Kaggle possible
    silver_arxiv = task(clean_arxiv_silver)
    silver_hn = task(clean_hn_silver)
    gold_arxiv = task(index_arxiv_gold)
    bronze_citations = task(ingest_citations_bronze, minutes=60)
    silver_citations = task(clean_citations_silver)

    spark_gold = SparkSubmitOperator(
        task_id="spark_gold_enrichment",
        application=f"{SRC}/scipulse/gold/spark_job.py",
        conn_id="spark_default",
        name="scipulse-gold",
        driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "1g"),
        executor_memory="1536m",
        executor_cores=2,
        conf={"spark.driver.host": "airflow-scheduler",
              "spark.driver.bindAddress": "0.0.0.0"},
        env_vars={"PYTHONPATH": SRC},
        execution_timeout=timedelta(minutes=90),
    )
    gold_checks = task(validate_gold, minutes=10)

    bronze_arxiv >> silver_arxiv >> [gold_arxiv, bronze_citations]
    bronze_citations >> silver_citations
    [silver_citations, silver_hn] >> spark_gold >> gold_checks
