"""Bronze ArXiv : dump Kaggle → MinIO `arxiv-raw/` (JSON Lines, lignes inchangées).

Choix de périmètre (documenté dans le README) :
  - le dump complet fait ~2,7 M articles / 5 Go ; une machine de 16 Go ne peut
    pas tout traiter avec Spark + Elastic. On constitue un **échantillon
    aléatoire reproductible** (reservoir sampling, graine fixe) des articles
    d'informatique (catégories cs.*) soumis depuis ARXIV_MIN_YEAR ;
  - un échantillon *uniforme* conserve les proportions entre années et
    catégories : les indicateurs de croissance restent non biaisés, ce qui ne
    serait pas le cas en prenant simplement les N premières lignes du fichier
    (triées par identifiant, donc les plus anciennes) ;
  - chaque ligne retenue est écrite **octet pour octet** : la sélection porte
    sur les enregistrements, jamais sur leur contenu (principe Bronze).
"""

from __future__ import annotations

import json
import os
import random
import shutil
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from scipulse.config import Bucket, Settings
from scipulse.http import make_session
from scipulse.monitoring import Layer, MetricsRecorder
from scipulse.storage import DataLake, partition_key

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"
DUMP_NAME = "arxiv-metadata-oai-snapshot.json"


@dataclass(frozen=True)
class SamplingConfig:
    sample_size: int = 100_000
    min_year: int = 2015
    category_prefixes: tuple[str, ...] = ("cs.",)
    seed: int = 42
    chunk_size: int = 10_000

    @classmethod
    def from_env(cls) -> SamplingConfig:
        """Surcharge par l'environnement : permet de lancer le pipeline sur un
        jeu de test (autre graine, plus petite taille) sans toucher au code."""
        defaults = cls()
        return cls(
            sample_size=int(os.environ.get("ARXIV_SAMPLE_SIZE", defaults.sample_size)),
            min_year=int(os.environ.get("ARXIV_MIN_YEAR", defaults.min_year)),
            seed=int(os.environ.get("ARXIV_SEED", defaults.seed)),
        )


def submission_year(record: dict) -> int | None:
    """Année de la première version (v1), ex. 'Mon, 2 Apr 2007 19:18:42 GMT' → 2007."""
    try:
        return parsedate_to_datetime(record["versions"][0]["created"]).year
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def is_eligible(record: dict, config: SamplingConfig) -> bool:
    """Article d'une catégorie ciblée, soumis à partir de l'année minimale."""
    categories = (record.get("categories") or "").split()
    if not any(c.startswith(config.category_prefixes) for c in categories):
        return False
    year = submission_year(record)
    return year is not None and year >= config.min_year


def reservoir_sample(lines: Iterable[str], config: SamplingConfig) -> tuple[list[str], int, int]:
    """Échantillon uniforme de taille fixe en une seule passe (algorithme R de Vitter).

    Mémoire O(sample_size) quelle que soit la taille du dump.
    Renvoie (lignes retenues, nb de lignes lues, nb de lignes éligibles).
    """
    rng = random.Random(config.seed)
    reservoir: list[str] = []
    read = eligible = 0
    for line in lines:
        read += 1
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue  # ligne corrompue : non éligible, comptée dans `read`
        if not is_eligible(record, config):
            continue
        eligible += 1
        if len(reservoir) < config.sample_size:
            reservoir.append(line)
        else:
            slot = rng.randrange(eligible)
            if slot < config.sample_size:
                reservoir[slot] = line
    return reservoir, read, eligible


def chunked(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def ensure_dump(data_dir: Path) -> Path:
    """Renvoie le chemin du dump local, en le téléchargeant depuis Kaggle s'il est absent."""
    target = data_dir / "arxiv" / DUMP_NAME
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    archive = target.parent / "arxiv.zip"
    with make_session().get(KAGGLE_URL, stream=True, timeout=60) as response:
        response.raise_for_status()
        with archive.open("wb") as handle:
            shutil.copyfileobj(response.raw, handle, length=16 * 1024 * 1024)
    with zipfile.ZipFile(archive) as zf:
        zf.extract(DUMP_NAME, target.parent)
    archive.unlink()
    return target


def run(settings: Settings | None = None, config: SamplingConfig | None = None) -> int:
    """Point d'entrée du DAG : échantillonne le dump et l'écrit dans arxiv-raw/."""
    settings = settings or Settings.from_env()
    config = config or SamplingConfig.from_env()
    lake = DataLake.from_settings(settings)
    recorder = MetricsRecorder(settings.metrics_dir)

    with recorder.track(pipeline="arxiv", layer=Layer.BRONZE, source="arxiv",
                        task="ingest_arxiv_bronze") as metrics:
        dump = ensure_dump(settings.data_dir)
        with dump.open(encoding="utf-8") as handle:
            sample, read, eligible = reservoir_sample(handle, config)

        # Tri par identifiant : fichiers stables d'un run à l'autre (même graine)
        sample.sort(key=lambda line: json.loads(line)["id"])
        run_at = datetime.now(UTC)
        written = sum(
            lake.put_lines(Bucket.ARXIV_RAW, partition_key("arxiv", run_at, i, "jsonl"), chunk)
            for i, chunk in enumerate(chunked(sample, config.chunk_size))
        )
        metrics.records_in = read
        metrics.records_out = written
        metrics.extra = {"eligible": eligible, "sample_size": config.sample_size,
                         "min_year": config.min_year, "seed": config.seed}
    return written
