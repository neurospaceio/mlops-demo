import polars as pl
import dagster as dg

@dg.asset(
    description="Get the training dataset",
    group_name="model_deployment",
)
def deployed_model(context: dg.AssetExecutionContext, trained_model) -> pl.DataFrame:
    pass