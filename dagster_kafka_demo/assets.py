import time
from typing import List

from dagster import (
    AssetSelection,
    Config,
    MaterializeResult,
    OpExecutionContext,
    asset,
    define_asset_job,
)


class MyAssetConfig(Config):
    batch: List[str]


@asset
def loaded_from_kafka(context: OpExecutionContext, config: MyAssetConfig) -> MaterializeResult:
    context.log.info(f"Handling kafka batch with values {config.batch}")

    # do the real processing here
    time.sleep(1)

    return MaterializeResult(
        metadata={
            "kafka_batch_size": len(config.batch),
            "kafka_batch_value_start": config.batch[0],
            "kafka_batch_value_end": config.batch[-1],
        }
    )


downstream_of_kafka = define_asset_job(
    name="downstream_of_kafka", selection=AssetSelection.assets(loaded_from_kafka)
)
