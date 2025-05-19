import argparse

from rabbitmq.publisher import CSVRecordPublisher

def main():
    parser = argparse.ArgumentParser(prog="publish_sensor_readings")
    parser.add_argument("-T", "--period", type=int)
    args = parser.parse_args()

    publisher = CSVRecordPublisher(
        queue_name="readings",
        filename="data/predictive_maintenance.csv",
        cols=["UDI", "Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]", "Torque [Nm]"]
    )

    publisher.run(period=args.period)

main()
