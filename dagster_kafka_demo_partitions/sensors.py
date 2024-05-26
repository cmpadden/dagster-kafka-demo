import json
from datetime import datetime
from typing import Mapping

from dagster import (
    DefaultSensorStatus,
    RunConfig,
    RunRequest,
    SensorResult,
    sensor,
)

from dagster_kafka_demo.resources import KafkaResource
from dagster_kafka_demo_partitions.assets import (
    KafkaConsumerConfig,
    kafka_consumer_output_job,
    kafka_consumer_partition_def,
)

MAX_BATCH_SIZE = 50
MAX_SENSOR_TICK_RUNTIME = 30
TIME_BETWEEN_SENSOR_TICKS = 40


def sensor_factory(replica_id: int):
    @sensor(
        minimum_interval_seconds=TIME_BETWEEN_SENSOR_TICKS,
        job_name=kafka_consumer_output_job.name,
        default_status=DefaultSensorStatus.RUNNING,
        name=f"watch_kafka_{replica_id}",
    )
    def watch_kafka(kafka: KafkaResource):
        """A sensor that consumes events from kafka and launches enqueues runs for them."""
        consumer = kafka.get_consumer()
        tstart = datetime.now()

        run_requests = []
        partition_keys = set()

        # todo - prevent batches from being too small
        while (datetime.now() - tstart).seconds < MAX_SENSOR_TICK_RUNTIME:
            msgs = consumer.poll(max_records=MAX_BATCH_SIZE, timeout_ms=2000)

            batches: Mapping[str, list] = {}
            max_offset = None

            for messages in msgs.values():
                for message in messages:
                    payload = json.loads(message.value.decode("utf-8"))
                    if payload.get("category") in batches:
                        batches[payload.get("category")].append(payload)
                    else:
                        batches[payload.get("category")] = [(payload)]
                    max_offset = message.offset

            for category, batch in batches.items():
                if len(batch) > 0:
                    partition_keys.add(category)
                    run_requests.append(
                        RunRequest(
                            run_key=f"max_offset_{max_offset}",
                            run_config=RunConfig(
                                ops={"loaded_from_kafka": KafkaConsumerConfig(batch=batch)}
                            ),
                            partition_key=category,
                        )
                    )

            consumer.commit()

        return SensorResult(
            run_requests=run_requests,
            dynamic_partitions_requests=[
                kafka_consumer_partition_def.build_add_request(list(partition_keys))
            ],
        )

    return watch_kafka
