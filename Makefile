PROJECT=

install:
	echo "motion quickstart"
	pip3 install poetry
	poetry install
	poetry shell

create:
	motion create
	
up:
	MOTION_API_TOKEN=$$(motion token) motion serve --name $(PROJECT)

# todo: run all the tests
test:
	echo "test"
