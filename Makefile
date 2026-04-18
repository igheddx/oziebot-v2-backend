.PHONY: dev-web compose-up compose-down api-install lint-python

dev-web:
	cd frontend/apps/web && npm run dev

compose-up:
	docker compose up --build

compose-down:
	docker compose down

compose-down-volumes:
	@echo "WARNING: This will DELETE all volumes including the database. All data will be lost."
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ] || (echo "Aborted." && exit 1)
	docker compose down -v

api-install:
	python -m venv .venv && . .venv/bin/activate && pip install -e backend/packages/domain -e backend/services/api

lint-python:
	ruff check backend/packages backend/services && ruff format --check backend/packages backend/services

seed-root:
	cd backend/services/api && PYTHONPATH=src DATABASE_URL=$${DATABASE_URL:?} SEED_ROOT_PASSWORD=$${SEED_ROOT_PASSWORD:?} python -m oziebot_api.scripts.seed_root_admin

seed-platform:
	cd backend/services/api && PYTHONPATH=src DATABASE_URL=$${DATABASE_URL:?} python -m oziebot_api.scripts.seed_platform_catalog

pytest-api:
	cd backend/services/api && pytest -v

run-market-data:
	cd backend/services/market-data-ingestor && python -m oziebot_market_data_ingestor
