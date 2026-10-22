"""Génère le fichier .env à partir de .env.example avec des secrets aléatoires.

Idempotent : si .env existe déjà, il n'est jamais écrasé (on ne veut pas
invalider les mots de passe d'une base PostgreSQL déjà initialisée).

Usage : python scripts/init_env.py
"""

from __future__ import annotations

import base64
import os
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Marqueurs reconnus dans .env.example et générateur associé
GENERATORS = {
    "__GENERATE__": lambda: secrets.token_urlsafe(24),
    # Clé Fernet = 32 octets aléatoires encodés en base64 url-safe
    "__FERNET__": lambda: base64.urlsafe_b64encode(os.urandom(32)).decode(),
}


def render_env(template: str) -> str:
    """Remplace chaque marqueur par une valeur fraîche (une par occurrence)."""
    lines = []
    for line in template.splitlines():
        for marker, generate in GENERATORS.items():
            while marker in line:
                line = line.replace(marker, generate(), 1)
        lines.append(line)
    return "\n".join(lines) + "\n"


def main() -> int:
    target = ROOT / ".env"
    if target.exists():
        print(".env existe déjà : conservé tel quel.")
        return 0
    template = (ROOT / ".env.example").read_text(encoding="utf-8")
    target.write_text(render_env(template), encoding="utf-8", newline="\n")
    print(".env généré avec des secrets aléatoires.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
