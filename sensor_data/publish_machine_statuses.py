import argparse

from rabbitmq.publisher import CSVRecordPublisher

def main():
    parser = argparse.ArgumentParser(prog="publish_machine_statuses")
    parser.add_argument("-T", "--period", type=float)
    args = parser.parse_args()

    publisher = CSVRecordPublisher(
        queue_name="daily_statuses",
        filename="data/predictive_maintenance.csv",
        cols=["UDI", "Target"]
    )

    publisher.run(period=args.period)

main()

