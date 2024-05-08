
clean: 
	rm -rf ~/.dagster_home; mkdir ~/.dagster_home; cp dagster.yaml ~/.dagster_home/dagster.yaml

dagster_dev: clean
	export DAGSTER_HOME="~/.dagster_home"; dagster dev -m kafkademo

check_lag: 
	rpk group describe group1

start_kafka:
	rpk container start

start_producer:
	python ./kafka_producer.py
