SHELL = /usr/bin/env bash -xeuo pipefail

format:
	poetry run isort src/ tests/
	poetry run black src/ tests/

test:
	PYTHONPATH=src \
	AWS_DEFAULT_REGION=ap-northeast-1 \
	poetry run pytest -v tests/unit

localstack-up:
	docker-compose up -d

localstack-down:
	docker-compose down
