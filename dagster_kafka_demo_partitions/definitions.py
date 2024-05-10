import os

from dagster import Definitions

from dagster_kafka_demo_partitions.assets import kafka_consumer_output_job, loaded_from_kafka
from dagster_kafka_demo_partitions.resources import KafkaResource
from dagster_kafka_demo_partitions.sensors import sensor_factory

SENSOR_REPLICAS = 4

defs = Definitions(
    assets=[loaded_from_kafka],
    jobs=[kafka_consumer_output_job],
    sensors=[sensor_factory(i) for i in range(SENSOR_REPLICAS)],
    resources={
        "kafka": KafkaResource(
            bootstrap_servers=[os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:52000")],
            topic_name=os.environ.get("KAFKA_TOPIC", "demo_topic_1"),
        )
    },
)
