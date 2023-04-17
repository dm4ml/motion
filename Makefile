install:
	echo "motion quickstart"
	pip3 install poetry
	poetry install
	poetry shell

create:
	motion create
	
up:
	MOTION_API_TOKEN=$$(motion token) motion serve --name motiononazure

# todo: run all the tests
test:
	echo "test"
