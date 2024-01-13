.PHONY: tests lint install mypy update docs

tests:
	poetry run pytest

lint:
	poetry run ruff motion/* --fix

install:
	pip install poetry
	poetry install --all-extras

mypy:
	poetry run mypy

update:
	poetry update

docs:
	poetry run mkdocs serve