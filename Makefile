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
	cd motionstate && \
	echo "Building motionstate" && \
	maturin build --release && \
	python -m venv ../.motionenv && \
	source ../.motionenv/bin/activate && pip install target/wheels/motionstate*.whl && deactivate && \
	echo "Copying .so file to motion" && \
	cp $$(find ../.motionenv -name "motionstate*.so") ../motion/ && \
	echo "Cleanup virtual environment..." && \
	rm -rf ../.motionenv && \
	echo "Done"
