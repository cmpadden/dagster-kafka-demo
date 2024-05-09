.PHONY: help

help:
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'

clean: 
	rm -rf ~/.dagster_home; mkdir ~/.dagster_home; cp dagster.yaml ~/.dagster_home/dagster.yaml

dagster_dev: clean  ## Start Dagster instance
	export DAGSTER_HOME="~/.dagster_home"; dagster dev -m dagster_kafka_demo

kafka_check_lag:   ## Display Redpanda Kafka group information
	rpk group describe group1

kafka_start:  ## Start Redpanda Kafka service container
	rpk container start

kafka_start_producer:  ## Start producer of Kafka messages
	python ./kafka_producer.py

ruff: ## Lint and fix code using Ruff
	-ruff check --fix .
	ruff format .
