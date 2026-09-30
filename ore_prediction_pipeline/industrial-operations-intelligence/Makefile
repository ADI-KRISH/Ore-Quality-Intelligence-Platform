.PHONY: setup test lint pipeline dashboard up down

# Spark/Delta need Linux + a matching Hadoop winutils.exe on Windows, which has
# no trustworthy first-party source. Local runs happen inside the app
# container instead; `up`/`down` only manage postgres.

setup:
	docker compose build

test:
	docker compose run --rm app pytest -q

lint:
	docker compose run --rm app sh -c "ruff check src tests && ruff format --check src tests"

pipeline:
	docker compose run --rm app python -m iop.cli run --stage all --config config/local.yaml

dashboard:
	docker compose run --rm --service-ports app streamlit run dashboard/app.py --server.address=0.0.0.0

up:
	docker compose up -d postgres

down:
	docker compose down
