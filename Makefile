.PHONY: install-requirements lint clean

#################################################################################
# GLOBALS                                                                       #
#################################################################################
SHELL := /bin/bash
PROJECT_NAME = match-analysis-transformer-refactoring
PYTHON = python3
PIP = pip
COVERAGE_THRESHOLD ?= 90

#################################################################################
# COMMANDS                                                                      #
#################################################################################

build:
	make install-requirements;
	make lint;
	make clean;

## Install Python Dependencies
install-requirements:
	${PYTHON} -m ${PIP} install --upgrade pip;
	${PYTHON} -m ${PIP} install -r requirements.txt

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

lint:
	pip install pylint;
	find . -type f -name "*.py" -not -path "./development_env/*" -not -path "Transfomers" | xargs pylint;

test:
	pip install pytest mock pytest-mock coverage;
	coverage run -m pytest;
	coverage report --fail-under=${COVERAGE_THRESHOLD}
