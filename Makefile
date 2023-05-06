.PHONY: tests lint install mypy update

tests:
	poetry run pytest

lint:
	poetry run ruff . --fix

install:
	pip install poetry
	poetry install

mypy:
	poetry run mypy

update:
	poetry update