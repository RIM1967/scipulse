"""Crée l'archive de rendu : prenom_nom_projet4_AIA01.zip

Contenu = fichiers suivis par Git (code, config, docs, notebooks, captures,
dashboards) + l'historique Git complet (dossier .git).
Exclus d'office : données (plusieurs Go),
secrets (.env), environnements virtuels, logs.

Usage : python scripts/make_archive.py prenom_nom
"""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def tracked_files() -> list[Path]:
    output = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, check=True,
                            capture_output=True).stdout.decode("utf-8")
    return [ROOT / name for name in output.split("\0") if name]


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 1
    archive = ROOT.parent / f"{sys.argv[1]}_projet4_AIA01.zip"
    prefix = Path("scipulse")
    files = tracked_files() + [p for p in (ROOT / ".git").rglob("*") if p.is_file()]

    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            if path.exists():
                zf.write(path, prefix / path.relative_to(ROOT))

    size_mb = archive.stat().st_size / 1_048_576
    print(f"{archive} — {len(files)} fichiers, {size_mb:.1f} Mo")
    return 0


if __name__ == "__main__":
    sys.exit(main())
