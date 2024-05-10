from typing import List

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
    batch: List[str]


# The above exception was caused by the following exception:
# dagster._check.CheckError: Failure condition: Dynamic partition key 127443 for partitions def 'batch' is invalid. After dynamic partitions requests are applied, it does not exist in the set of valid partition keys.

kafka_consumer_partition_def = DynamicPartitionsDefinition(name="batch")

@asset(partitions_def=kafka_consumer_partition_def)
def loaded_from_kafka(
    context: OpExecutionContext, config: KafkaConsumerConfig
) -> MaterializeResult:
    context.log.info(f"handling kafka batch: {context.partition_key}")

    # write file with records, partitioned by min/max batch ids
    with open(f"data/{context.partition_key}", "w") as f:
        f.writelines(config.batch)

    return MaterializeResult(
        metadata={
            "kafka_batch_size": len(config.batch),
            "kafka_batch_value_start": config.batch[0],
            "kafka_batch_value_end": config.batch[-1],
        }
    )


kafka_consumer_output_job = define_asset_job(
    name="kafka_consumer_output", selection=AssetSelection.assets(loaded_from_kafka)
)
