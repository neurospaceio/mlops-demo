import argparse

from rabbitmq import CSVRecordPublisher

def main():
    parser = argparse.ArgumentParser(prog="publish_sensor_failures")
    parser.add_argument("-T", "--period", type=int)
    args = parser.parse_args()

    publisher = CSVRecordPublisher(
        queue_name="failures",
        filename="data/predictive_maintenance.csv",
        cols=["UDI", "Target"]
    )

    publisher.run(period=args.period)

main()

