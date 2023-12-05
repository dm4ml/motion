.PHONY: tests lint install mypy update docs build

tests:
	poetry run pytest

lint:
	poetry run ruff motion/* --fix

install:
	pip install poetry
	poetry install

mypy:
	poetry run mypy

update:
	poetry update

docs:
	poetry run mkdocs serve

build:
	poetry run maturin develop --release
