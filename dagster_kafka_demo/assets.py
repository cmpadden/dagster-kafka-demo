from typing import List

from dagster import (
    AssetSelection,
    Config,
    MaterializeResult,
    OpExecutionContext,
    asset,
    define_asset_job,
)


class KafkaConsumerConfig(Config):
    max_offset: str
    batch: List[str]


@asset
def loaded_from_kafka(
    context: OpExecutionContext, config: KafkaConsumerConfig
) -> MaterializeResult:
    context.log.info(f"Handling kafka batch with values {config.batch}")

    # write file with records, partitioned by min/max batch ids
    with open(f"data/{config.max_offset}", "w") as f:
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
