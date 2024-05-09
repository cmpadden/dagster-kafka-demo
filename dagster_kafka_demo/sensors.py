from datetime import datetime

from dagster import (
    DefaultSensorStatus,
    RunConfig,
    RunRequest,
    sensor,
)

from dagster_kafka_demo.assets import KafkaConsumerConfig, kafka_consumer_output_job
from dagster_kafka_demo.resources import KafkaResource

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
        # for each sensor loop, create a consumer and read up to MAX records
        consumer = kafka.get_consumer()
        tstart = datetime.now()

        while (datetime.now() - tstart).seconds < MAX_SENSOR_TICK_RUNTIME:
            msgs = consumer.poll(max_records=MAX_BATCH_SIZE, timeout_ms=2000)

            batch = []
            max_offset = None

            for messages in msgs.values():
                for message in messages:
                    batch.append(message.value.decode("utf-8"))
                    max_offset = message.offset

            if len(batch) > 0:
                yield RunRequest(
                    run_key=f"max_offset_{max_offset}",
                    run_config=RunConfig(
                        ops={
                            "loaded_from_kafka": KafkaConsumerConfig(
                                max_offset=str(max_offset), batch=batch
                            )
                        }
                    ),
                )

            consumer.commit()

    return watch_kafka
