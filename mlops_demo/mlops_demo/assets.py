import polars as pl
import json
import pika
import dagster as dg

asset_groups = {
    "ingestion": "data_ingestion",
    "training": "model_training",
    "inference": "model_inference"
}


@dg.asset(
    description="Gets all sensor readings currently in the queue. These readings are streamed continuously from all sensors.",
    required_resource_keys={"rmqconn"},
    group_name=asset_groups["ingestion"],
    kinds={"rabbitmq"}
)
def sensor_readings(context: dg.AssetExecutionContext) -> list[str]:

    rmqconn = context.resources.rmqconn

    queue = "readings"
    channel = rmqconn.channel()
    channel.queue_declare(queue=queue)

    messages = []
    
    # Drain the queue until it's empty
    while True:
        method_frame, header_frame, body = channel.basic_get(queue=queue, auto_ack=True)
        if method_frame:
            msg = body.decode("utf-8")
            context.log.info(f"Received message: {msg}")
            messages.append(msg)
        else:
            context.log.info("No more messages in queue.")
            break

    if len(messages) > 0:
        context.add_output_metadata({
            "message": messages[0],
            "num_messages": len(messages)
        })

    return messages

@dg.asset(
    description="Convert readings to dataframe and clean",
    group_name=asset_groups["ingestion"],
)
def cleaned_readings(context: dg.AssetExecutionContext, sensor_readings: list[str]) -> pl.DataFrame:
    data = [json.loads(row) for row in sensor_readings]
    df = pl.from_records(data)
    return df

@dg.asset(
    description="Convert failures to dataframe and clean",
    group_name=asset_groups["ingestion"],
)
def cleaned_failures(context: dg.AssetExecutionContext, machine_failures: list[str]) -> pl.DataFrame:
    data = [json.loads(row) for row in machine_failures]
    df = pl.from_records(data)
    return df



@dg.asset(
    description="Get the training dataset",
    group_name=asset_groups["training"],
)
def training_data(context: dg.AssetExecutionContext, joined_cleaned_readings_and_failures: pl.DataFrame) -> pl.DataFrame:
    pass
    
@dg.asset(
    description="Get the test dataset",
    group_name=asset_groups["training"],
)
def test_data(context: dg.AssetExecutionContext, joined_cleaned_readings_and_failures: pl.DataFrame) -> pl.DataFrame:
    pass



@dg.asset(
    description="Gets all failures currently in the queue. The failures are streamed asynchronously with the readings.",
    required_resource_keys={"rmqconn"},
    group_name=asset_groups["ingestion"],
    kinds={"rabbitmq"}
)
def machine_failures(context: dg.AssetExecutionContext) -> list[str]:

    rmqconn = context.resources.rmqconn

    queue = "failures"
    channel = rmqconn.channel()
    channel.queue_declare(queue=queue)

    messages = []
    
    # Drain the queue until it's empty
    while True and len(messages) < 200:
        method_frame, header_frame, body = channel.basic_get(queue=queue, auto_ack=True)
        if method_frame:
            msg = body.decode("utf-8")
            context.log.info(f"Received message: {msg}")
            messages.append(msg)
        else:
            context.log.info("No more messages in queue {}.")
            break

    if len(messages) > 0:
        context.add_output_metadata({
            "message": messages[0],
            "num_messages": len(messages)
        })

    return messages

get_all_readings_job = dg.define_asset_job("get_all_readings_job", selection=[sensor_readings])

@dg.sensor(
    job=get_all_readings_job,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING,
    required_resource_keys={"rmqconn"}
)
def min_number_of_readings_sensor(context: dg.SensorEvaluationContext):

    rmqconn = context.resources.rmqconn

    channel = rmqconn.channel()
    queue = channel.queue_declare(queue="readings")
    message_count = queue.method.message_count

    if message_count > 100:
        yield dg.RunRequest()
    else:
        yield dg.SkipReason(f"Minimum number of messages not reached yet (it was {message_count})")

get_all_failures_job = dg.define_asset_job("get_all_failures_job", selection=[machine_failures])

@dg.sensor(
    job=get_all_failures_job,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING,
    required_resource_keys={"rmqconn"}
)
def min_number_of_failures_sensor(context: dg.SensorEvaluationContext):

    rmqconn = context.resources.rmqconn

    channel = rmqconn.channel()
    queue = channel.queue_declare(queue="failures")
    message_count = queue.method.message_count

    if message_count > 50:
        yield dg.RunRequest()
    else:
        yield dg.SkipReason(f"Minimum number of messages not reached yet (it was {message_count})")


@dg.asset(
    description="",
    group_name=asset_groups["ingestion"],
    kinds={"polars"}
)
def joined_cleaned_readings_and_failures(context: dg.AssetExecutionContext, cleaned_readings: list[str], cleaned_failures: list[str]) -> int:

    readings = [json.loads(item) for item in cleaned_readings]
    failures = [json.loads(item) for item in cleaned_failures] # here we only get failures up to a specific time, we assume everything else succeeded

    df_readings = pl.DataFrame(readings)
    df_failures = pl.DataFrame(failures)

    joined = df_readings.join(df_failures, on="UDI", how="inner")

    context.add_output_metadata({
        "num_matches": len(joined)
    })

    return len(joined)

@dg.asset(
    description="Inference, outputs the devices at risk of failure",
    group_name=asset_groups["inference"]
)
def devices_at_risk(context: dg.AssetExecutionContext, cleaned_readings: list[str]) -> list[str]:
    readings = [json.loads(item) for item in cleaned_readings]
    df_readings = pl.DataFrame(readings)

    return [""]

@dg.asset(
    description="",
    group_name=asset_groups["training"]
)
def train() -> int:
    return 0