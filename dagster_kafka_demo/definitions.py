from dagster import Definitions

from dagster_kafka_demo.assets import downstream_of_kafka, loaded_from_kafka
from dagster_kafka_demo.resources import KafkaResource
from dagster_kafka_demo.sensors import sensor_factory

SENSOR_REPLICAS = 4

defs = Definitions(
    assets=[loaded_from_kafka],
    jobs=[downstream_of_kafka],
    sensors=[sensor_factory(i) for i in range(SENSOR_REPLICAS)],
    resources={
        "kafka": KafkaResource(bootstrap_servers=["localhost:52000"], topic_name="First_Topic")
    },
)
