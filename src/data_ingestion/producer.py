import os, socket, gzip, json, csv
from pathlib import Path
from io import StringIO
from typing import Callable

from confluent_kafka import Producer


# docker run -d -p 9092:9092 apache/kafka:3.7.0
class MessageProducer:
    """
    Produces messages to Kafka messaging system.
    Producer reads files in local storage and produces a message for each line in the file
    """

    def __init__(self, name: str, data_dir: str = "./dataset_clean") -> None:
        """
        Init message producer
        """

        self.name = name
        self.data_dir = data_dir

        # Use local kafka server
        self.producer = Producer(
            {
                "bootstrap.servers": "localhost:9092,localhost:9092",
                "client.id": socket.gethostname(),
            }
        )

        # Topic name
        self.topic = f"{name}_queue"

    def publish_file_event(self, msg_content: dict) -> None:
        """
        Publishes a message to message queue.
        Each message is a row of data from a csv file.
        """
        event_json = json.dumps(msg_content)
        self.producer.produce(topic=self.topic, value=event_json)

        print(f" [x] Sent event '{1}' with topic '{self.topic}' and key '{1}'")

    def publish_file_events(self) -> None:
        """
        Publish file events for files not already in mongodb.
        """
        csv_files = Path(self.data_dir)

        # Iterate files
        for f in csv_files.glob("*.csv"):
            # Fetch and transform file

            with f.open("r") as csv_file:

                header = csv_file.readline()[:-1]
                reader = csv.DictReader(csv_file, fieldnames=header.split(","))

                for row in reader:
                    self.publish_file_event(row)

            self.producer.flush()

    def close(self) -> None:
        """
        Close connection
        """
        self.producer.close()


if __name__ == "__main__":
    p = MessageProducer("1", "test_clean")
    p.publish_file_events()
