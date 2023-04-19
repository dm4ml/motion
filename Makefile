PROJECT=
PYTHON_BINARY=python3.9
PYTHON_BINARY_EXISTS := $(shell command -v $(PYTHON_BINARY) 2> /dev/null)

install:
ifndef PYTHON_BINARY_EXISTS
    $(error "$(PYTHON_BINARY) is not found; please install it or change the PYTHON_BINARY variable")
endif
	poetry env use $(PYTHON_BINARY)
	pip3 install poetry
	poetry install
	poetry shell
	echo "motion installed! now run 'make create' to create a new project, and 'make up' to run it."

create:
	motion create

up:
	poetry shell
	MOTION_API_TOKEN=$$(motion token) motion serve --name $(PROJECT)

test:
	poetry run pytest

mypy:
	poetry run mypy

lint:
	poetry run ruff .