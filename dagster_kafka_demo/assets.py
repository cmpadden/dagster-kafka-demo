import time
from typing import List

from dagster import AssetSelection, Config, OpExecutionContext, asset, define_asset_job


class MyAssetConfig(Config):
    batch: List[str]


@asset
def loaded_from_kafka(context: OpExecutionContext, config: MyAssetConfig):
    context.log.info(f"Handling kafka batch with values {config.batch}")

    context.add_output_metadata(metadata={"kafka_message_value": str(config.batch)})

    # do the real processing here
    time.sleep(1)


downstream_of_kafka = define_asset_job(
    name="downstream_of_kafka", selection=AssetSelection.assets(loaded_from_kafka)
)
