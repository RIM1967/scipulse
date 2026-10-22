# =============================================================================
#  SciPulse — commandes du projet
#  Windows : installer GNU Make une fois (winget install ezwinports.make)
# =============================================================================

PYTHON ?= python
COMPOSE = docker compose

.PHONY: help env build up down clean ps logs check test lint

help:  ## Affiche cette aide
	@$(PYTHON) -c "import re;[print(f'  {m[0]:<8} {m[1]}') for m in re.findall(r'^(\w+):.*?## (.*)$$', open('Makefile', encoding='utf-8').read(), re.M)]"

env:  ## Génère .env (secrets aléatoires) s'il n'existe pas
	$(PYTHON) scripts/init_env.py

build: env  ## Construit les images (Airflow, Spark, MinIO)
	$(COMPOSE) build

up: env  ## Démarre toute la stack de zéro et attend qu'elle soit saine
	$(COMPOSE) up -d --build
	$(PYTHON) scripts/check_stack.py --wait 600

down:  ## Arrête la stack (données conservées)
	$(COMPOSE) down

clean:  ## Arrête la stack ET supprime les volumes (repart de zéro)
	$(COMPOSE) down -v

ps:  ## État des conteneurs
	$(COMPOSE) ps

logs:  ## Logs en continu (ex. make logs s=airflow-scheduler)
	$(COMPOSE) logs -f $(s)

check:  ## Vérifie la santé de tous les services
	$(PYTHON) scripts/check_stack.py

test:  ## Lance les tests unitaires
	$(PYTHON) -m pytest

lint:  ## Analyse statique (ruff)
	$(PYTHON) -m ruff check .
