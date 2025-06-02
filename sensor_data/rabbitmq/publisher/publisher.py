import polars as pl
import pika
import time
import json

class CSVRecordPublisher:

    def __init__(self, queue_name, filename, cols):
        self._queue_name = queue_name
        self._df: pl.DataFrame = pl.read_csv(filename)[cols]

        self._connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self._channel = self._connection.channel()
        self._channel.queue_declare(self._queue_name)

    def run(self, period=None, num_records=None, offset=0):
        print(num_records)
        print(self._df.height)
        print(offset)
        if num_records is not None:
            upper_bound = min(offset+num_records, self._df.height)
        else:
            upper_bound = self._df.height

        df = self._df[offset:upper_bound]

        for reading in df.iter_rows(named=True):
            body = json.dumps(reading)
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=body
            )

            if period is not None:
                time.sleep(period)

        self._connection.close()
