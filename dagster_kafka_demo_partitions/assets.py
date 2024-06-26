import tempfile
from typing import Any, List

from dagster import (
    AssetSelection,
    Config,
    MaterializeResult,
    OpExecutionContext,
    asset,
    define_asset_job,
)
from dagster._core.definitions import DynamicPartitionsDefinition


class KafkaConsumerConfig(Config):
    batch: List[Any]


kafka_consumer_partition_def = DynamicPartitionsDefinition(name="category")


@asset(partitions_def=kafka_consumer_partition_def)
def loaded_from_kafka(
    context: OpExecutionContext, config: KafkaConsumerConfig
) -> MaterializeResult:
    context.log.info(f"handling kafka batch: {context.partition_key}")

    # Processing of batch records is to be done here...
    with tempfile.TemporaryFile(mode="w") as tf:
        tf.writelines(config.batch)

    return MaterializeResult(
        metadata={
            "kafka_batch_size": len(config.batch),
            # "kafka_batch_value_start": config.batch[0].get("value"),
            # "kafka_batch_value_end": config.batch[-1].get("value"),
        }
    )


kafka_consumer_output_job = define_asset_job(
    name="kafka_consumer_output", selection=AssetSelection.assets(loaded_from_kafka)
)
