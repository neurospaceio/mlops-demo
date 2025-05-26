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
    kinds={"rabbitmq"},
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
    description="Gets all statuses currently in the queue. The statuses are streamed asynchronously with the readings by the end of the day.",
    required_resource_keys={"rmqconn"},
    group_name=asset_groups["ingestion"],
    kinds={"rabbitmq"}
)
def daily_machine_statuses(context: dg.AssetExecutionContext) -> list[str]:

    rmqconn = context.resources.rmqconn

    queue = "machine_statuses"
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
            context.log.info("No more messages in queue {}.")
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
    description="Convert statuses to dataframe and clean",
    group_name=asset_groups["ingestion"],
)
def cleaned_daily_statuses(context: dg.AssetExecutionContext, daily_machine_statuses: list[str]) -> pl.DataFrame:
    data = [json.loads(row) for row in daily_machine_statuses]
    df = pl.from_records(data)
    return df




@dg.asset(
    description="",
    group_name=asset_groups["ingestion"],
    kinds={"polars"}
)
def joined_cleaned_readings_and_statuses(context: dg.AssetExecutionContext, cleaned_readings: pl.DataFrame, cleaned_daily_statuses: pl.DataFrame) -> pl.DataFrame:

    context.log.info(f"cleaned_readings.columns: {cleaned_readings.columns}")
    context.log.info(f"cleaned_daily_statuses.columns: {cleaned_daily_statuses.columns}")
    context.log.info(f"len(cleaned_readings): {len(cleaned_readings)}")
    context.log.info(f"len(cleaned_daily_statuses): {len(cleaned_daily_statuses)}")

    df_joined = cleaned_readings.join(cleaned_daily_statuses, on="UDI", how="inner")

    context.add_output_metadata({
        "num_matches": len(df_joined)
    })

    return df_joined


get_all_readings_job = dg.define_asset_job("get_all_readings_job", selection=[sensor_readings, cleaned_readings, joined_cleaned_readings_and_statuses])

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

get_all_daily_machine_statuses_job = dg.define_asset_job("get_all_daily_machine_statuses_job", selection=[daily_machine_statuses, cleaned_daily_statuses, joined_cleaned_readings_and_statuses])

@dg.sensor(
    job=get_all_daily_machine_statuses_job,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING,
    required_resource_keys={"rmqconn"}
)
def min_number_of_machine_statuses_sensor(context: dg.SensorEvaluationContext):

    rmqconn = context.resources.rmqconn

    channel = rmqconn.channel()
    queue = channel.queue_declare(queue="daily_statuses")
    message_count = queue.method.message_count

    if message_count > 50:
        yield dg.RunRequest()
    else:
        yield dg.SkipReason(f"Minimum number of messages not reached yet (it was {message_count})")


@dg.asset(
    description="Inference, outputs the devices at risk of failure",
    group_name=asset_groups["inference"]
)
def devices_at_risk(context: dg.AssetExecutionContext, cleaned_readings: pl.DataFrame, deployed_model) -> list[str]:
    readings = [json.loads(item) for item in cleaned_readings]
    df_readings = pl.DataFrame(readings)

    return [""]
