"""Porte qualité commune aux trois sources Silver.

Règle : on valide *avant* de publier. Si la suite GX échoue, la partition
Silver existante n'est pas écrasée (les consommateurs gardent la dernière
version saine), la métrique passe en `quality_failed` et la tâche échoue,
ce qui déclenche l'alerte Airflow et bloque les tâches Gold en aval.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from scipulse.monitoring import RunMetrics, Status
from scipulse.quality.runner import ValidationOutcome, validate
from scipulse.storage import DataLake

if TYPE_CHECKING:
    import pandas as pd

ROWS_PER_FILE = 50_000


class QualityGateError(RuntimeError):
    """Une suite Great Expectations a échoué : publication Silver bloquée."""


def publish(df: pd.DataFrame, *, lake: DataLake, bucket: str, prefix: str,
            suite: str, metrics: RunMetrics) -> ValidationOutcome:
    """Valide puis (si OK) remplace atomiquement la partition Silver par `df`."""
    outcome = validate(df, suite)
    metrics.gx_success_percent = outcome.success_percent
    metrics.gx_failed_expectations = outcome.failed_expectations
    if not outcome.success:
        metrics.status = Status.QUALITY_FAILED
        raise QualityGateError(f"{suite} : échec de {outcome.failed_expectations}")

    lake.delete_prefix(bucket, f"{prefix}/")
    for part, start in enumerate(range(0, max(len(df), 1), ROWS_PER_FILE)):
        lake.put_parquet(bucket, f"{prefix}/part-{part:04d}.parquet",
                         df.iloc[start:start + ROWS_PER_FILE])
    metrics.records_out = len(df)
    return outcome
