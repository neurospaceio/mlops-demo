import dagster as dg

from mlops_demo import assets  # noqa: TID252
from mlops_demo import training
from mlops_demo import deployment
from mlops_demo import resources
from mlops_demo.assets import get_all_readings_job, min_number_of_readings_sensor, min_number_of_machine_statuses_sensor


all_assets = dg.load_assets_from_modules([assets, training, deployment])

defs = dg.Definitions(
    assets=all_assets,
    resources = {
        "rmqconn":  resources.rabbitmq_connection_resource.configured({"host": "localhost"})
    },
    jobs=[get_all_readings_job],
    sensors=[min_number_of_readings_sensor, min_number_of_machine_statuses_sensor]
)
