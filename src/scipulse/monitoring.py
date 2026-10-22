"""Métriques d'exécution du pipeline (monitoring D-06).

Chaque tâche enregistre une ligne JSON par exécution dans
`$METRICS_DIR/metrics-AAAA-MM-JJ.jsonl`. Logstash suit ces fichiers et les
indexe dans `scipulse-pipeline-metrics`, qui alimente le dashboard Kibana
de monitoring : volumes par couche, taux de succès GX, fraîcheur, erreurs.

Pourquoi un fichier plutôt qu'une écriture directe dans Elasticsearch ?
Découplage : une tâche ne doit jamais échouer parce que le monitoring est
indisponible. Logstash rattrape les lignes en attente à son redémarrage.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Status(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    # Données écrites mais suite GX en échec : surveillé et alerté
    QUALITY_FAILED = "quality_failed"


@dataclass
class RunMetrics:
    """Une exécution de tâche. Les compteurs sont renseignés par la tâche elle-même."""

    pipeline: str
    layer: Layer
    source: str
    task: str
    status: Status = Status.SUCCESS
    records_in: int = 0
    records_out: int = 0
    records_rejected: int = 0
    duration_s: float = 0.0
    gx_success_percent: float | None = None
    gx_failed_expectations: list[str] = field(default_factory=list)
    # Date la plus récente présente dans les données (fraîcheur métier)
    data_watermark: str | None = None
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


class MetricsRecorder:
    def __init__(self, metrics_dir: Path) -> None:
        self.metrics_dir = metrics_dir

    def record(self, metrics: RunMetrics) -> Path:
        """Ajoute la métrique au fichier du jour.

        Une ligne courte écrite en mode append est atomique sous Linux : des
        tâches Airflow parallèles peuvent écrire dans le même fichier.
        """
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        path = self.metrics_dir / f"metrics-{datetime.now(UTC):%Y-%m-%d}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(metrics), ensure_ascii=False, default=str) + "\n")
        return path

    @contextmanager
    def track(self, *, pipeline: str, layer: Layer, source: str, task: str
              ) -> Iterator[RunMetrics]:
        """Mesure durée et statut d'un bloc, et enregistre la métrique même en cas d'erreur.

        Usage :
            with recorder.track(pipeline="arxiv", layer=Layer.BRONZE,
                                source="arxiv", task="ingest") as m:
                m.records_out = ingest()
        """
        metrics = RunMetrics(pipeline=pipeline, layer=layer, source=source, task=task)
        start = time.perf_counter()
        try:
            yield metrics
        except Exception as exc:
            # Un échec qualité déjà qualifié par la tâche garde son statut
            if metrics.status == Status.SUCCESS:
                metrics.status = Status.FAILED
            metrics.error = f"{type(exc).__name__}: {exc}"[:500]
            raise
        finally:
            metrics.duration_s = round(time.perf_counter() - start, 3)
            self.record(metrics)
