.PHONY: install test run sync-corps sync-fin screener report clean

install:
	python -m uv sync --group dev

install-all:
	python -m uv sync --group dev --group data --group ui --group reports --group stats

test:
	python -m uv run pytest tests/unit/ -v

test-all:
	python -m uv run pytest tests/ -v

run:
	python -m uv run streamlit run src/krqs/ui/app.py

sync-corps:
	python -m uv run python scripts/sync_corp_codes.py

sync-fin:
	python -m uv run python scripts/sync_financials.py $(ARGS)

screener:
	python -m uv run python scripts/run_screener.py

report:
	python -m uv run python scripts/generate_report.py $(ARGS)

clean:
	rm -rf .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
