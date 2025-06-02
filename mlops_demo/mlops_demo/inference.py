import polars as pl
import json
import pika
import dagster as dg

group_name = "inference"

@dg.asset(
    description="Inference, outputs the devices at risk of failure",
    group_name=group_name
)
def devices_at_risk(context: dg.AssetExecutionContext, cleaned_readings: pl.DataFrame, deployed_model) -> list[str]:
    readings = [json.loads(item) for item in cleaned_readings]
    df_readings = pl.DataFrame(readings)

    return [""]