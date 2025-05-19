import polars as pl
import pika
import time
import json

class CSVRecordPublisher:

    def __init__(self, queue_name, filename, cols):
        self._queue_name = queue_name
        self._df = pl.read_csv(filename)[cols]

        self._connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self._channel = self._connection.channel()
        self._channel.queue_declare(self._queue_name)

    def run(self, period):

        for reading in self._df.iter_rows(named=True):
            body = json.dumps(reading)
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=body
            )
            time.sleep(period)

        # We probably won't ever reach this point... whatever...
        self._connection.close()
