.PHONY: setup fmt lint test

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

fmt:
	python -m pip install ruff black || true
	ruff check --select I --fix . || true
	black . || true

lint:
	python -m pip install ruff || true
	ruff check . || true

test:
	python -m pip install pytest || true
	pytest -q
