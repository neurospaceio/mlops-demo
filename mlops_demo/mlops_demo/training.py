import polars as pl
import dagster as dg

asset_groups = {
    "ingestion": "data_ingestion",
    "training": "model_training",
    "inference": "model_inference"
}

train_fraction = 0.7
test_fraction = 1 - train_fraction

training_data_file = "training_data.jsonl"
test_data_file = "test_data.jsonl"

@dg.asset(
    description="Get the training dataset",
    group_name=asset_groups["training"],
)
def training_data(context: dg.AssetExecutionContext, joined_cleaned_readings_and_statuses: pl.DataFrame) -> pl.DataFrame:

    try:
        with open(training_data_file, "r") as f:
            df1 = pl.read_ndjson(f)
    except FileNotFoundError as e:
        df1 = pl.DataFrame()

    previous_num_train_rows = len(df1)

    df2 = joined_cleaned_readings_and_statuses

    upper_index = int(len(df2)*train_fraction)
    df_train = df1.vstack(df2[0:upper_index])

    new_num_train_rows = len(df_train)

    context.log.info(f"Number of new training rows: {previous_num_train_rows}")
    context.log.info(f"Number of training rows: {new_num_train_rows}")
    
    with open(training_data_file, "w+") as f:
        df_train.write_ndjson(f)

    return df_train
    
    
@dg.asset(
    description="Get the test dataset",
    group_name=asset_groups["training"],
)
def test_data(context: dg.AssetExecutionContext, joined_cleaned_readings_and_statuses: pl.DataFrame) -> pl.DataFrame:
    try:
        with open(test_data_file, "r") as f:
            df1 = pl.read_ndjson(f)
    except FileNotFoundError as e:
        df1 = pl.DataFrame()

    previous_num_test_rows = len(df1)

    df2 = joined_cleaned_readings_and_statuses

    lower_bound = int(len(df2)*train_fraction)
    df_test = df1.vstack(df2[lower_bound:])

    new_num_test_rows = len(df_test)

    context.log.info(f"Number of new test rows: {previous_num_test_rows}")
    context.log.info(f"Number of test rows: {new_num_test_rows}")
    
    with open(test_data_file, "w+") as f:
        df_test.write_ndjson(f)

    return df_test


@dg.asset(
    description="",
    group_name=asset_groups["training"]
)
def trained_model(context: dg.AssetExecutionContext, training_data: pl.DataFrame, test_data: pl.DataFrame) -> list[str]:


    return 0