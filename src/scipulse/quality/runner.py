"""Exécution des suites Great Expectations 1.x et publication des Data Docs.

Contexte GX *fichier* (dossier `gx/` versionné) : suites, définitions de
validation et checkpoints sont persistés et inspectables. Les Data Docs
sont générées en local (gx/uncommitted/data_docs/local_site) puis copiées
dans le bucket MinIO `gx-data-docs`, servi en lecture publique :
    http://localhost:9000/gx-data-docs/index.html
(GX 1.x ne fournit plus de backend S3 pour les Data Docs : la copie est
faite par notre code, avec le client MinIO commun.)
"""

from __future__ import annotations

import mimetypes
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scipulse.config import Bucket, Settings
from scipulse.quality.suites import SUITES
from scipulse.storage import DataLake

if TYPE_CHECKING:
    import pandas as pd

# GX crée son dossier `gx/` *dans* project_root_dir : on lui passe la racine du dépôt
PROJECT_ROOT = Path(__file__).resolve().parents[3]
LOCAL_SITE = Path("gx") / "uncommitted" / "data_docs" / "local_site"


@dataclass
class ValidationOutcome:
    suite: str
    success: bool
    success_percent: float
    evaluated: int
    failed_expectations: list[str] = field(default_factory=list)


def publish_data_docs(site_dir: Path, lake: DataLake) -> int:
    """Copie le site Data Docs local dans le bucket public MinIO. Renvoie le nb de fichiers."""
    count = 0
    for path in site_dir.rglob("*"):
        if path.is_file():
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            lake.client.put_object(Bucket=Bucket.GX_DATA_DOCS.value,
                                   Key=path.relative_to(site_dir).as_posix(),
                                   Body=path.read_bytes(), ContentType=content_type)
            count += 1
    return count


def validate(df: pd.DataFrame, suite_name: str, settings: Settings | None = None,
             project_root: Path = PROJECT_ROOT) -> ValidationOutcome:
    """Valide un DataFrame contre une suite, met à jour et publie les Data Docs."""
    import great_expectations as gx

    settings = settings or Settings.from_env()
    context: Any = gx.get_context(mode="file", project_root_dir=str(project_root))

    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name=suite_name, expectations=SUITES[suite_name]()))
    source = context.data_sources.add_or_update_pandas(name="scipulse_silver")
    asset = source.add_dataframe_asset(name=suite_name)
    batch = asset.add_batch_definition_whole_dataframe(name=f"{suite_name}_batch")
    definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{suite_name}_validation", data=batch, suite=suite))
    checkpoint = context.checkpoints.add_or_update(gx.Checkpoint(
        name=f"{suite_name}_checkpoint",
        validation_definitions=[definition],
        actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
        result_format={"result_format": "SUMMARY"},
    ))

    result = checkpoint.run(batch_parameters={"dataframe": df})
    publish_data_docs(project_root / LOCAL_SITE, DataLake.from_settings(settings))

    run = next(iter(result.run_results.values()))
    stats = run.statistics
    failed = [
        f"{r.expectation_config.type}({r.expectation_config.kwargs.get('column', '')})"
        for r in run.results if not r.success
    ]
    return ValidationOutcome(
        suite=suite_name,
        success=bool(result.success),
        success_percent=round(float(stats.get("success_percent") or 0.0), 2),
        evaluated=int(stats.get("evaluated_expectations", 0)),
        failed_expectations=failed,
    )
