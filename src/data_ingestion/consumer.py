import json

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import from_json, col, window, avg, struct, to_json
from pyspark.sql.streaming import StreamingQueryListener
from pymongo.mongo_client import MongoClient


class StreamListener(StreamingQueryListener):
    """
    Class that listens to the datastream
    """

    def __init__(self, name: str, db_port: str = "27017") -> None:

        self.name = name

        uri = f"mongodb://localhost:{db_port}"
        self.mongo_client = MongoClient(uri)

    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):

        print("Query progressed")

        # Convert json to dict
        progress_event = str(event.progress)
        event_dict = json.loads(progress_event)

        # If rows were processed, insert metrics to database
        if event_dict["numInputRows"] > 0:
            db = self.mongo_client[self.name]
            collection = db["metrics"]
            collection.insert_one(event_dict)

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")


class StreamApp:
    def __init__(self, tenant: str, n_threads: int = 2, db_port: str = "27017") -> None:

        self.name = tenant
        self.MONGO_URI = f"mongodb://127.0.0.1:{db_port}"

        # Spark session
        self.spark = (
            SparkSession.builder.master(f"local[{n_threads}]")
            .appName("TenantStreamApp")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate()
        )

        self.spark.streams.addListener(StreamListener(tenant, db_port))
        self.spark.sparkContext.setLogLevel("WARN")

        # Data schema
        self.schema = StructType(
            [
                StructField("time", LongType()),
                StructField("readable_time", TimestampType()),
                StructField("acceleration", DoubleType()),
                StructField("acceleration_x", IntegerType()),
                StructField("acceleration_y", IntegerType()),
                StructField("acceleration_z", IntegerType()),
                StructField("battery", IntegerType()),
                StructField("humidity", DoubleType()),
                StructField("pressure", DoubleType()),
                StructField("temperature", DoubleType()),
            ]
        )

    def read_kafka_stream(self, topic: str):
        """
        Subscribe to Kafka topic, creating a data stream
        """

        kafka_params = {
            "kafka.bootstrap.servers": "localhost:9092,localhost:9092",
            "subscribe": topic,
        }

        # Read kafka stream with given parameters
        kafka_stream = (
            self.spark.readStream.format("kafka").options(**kafka_params).load()
        )

        # Convert stream into dataframe
        dataframe = (
            kafka_stream.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.schema).alias("data"))
            .select("data.*")
        )

        return dataframe

    def key_stream(self, topic):
        kafka_params = {
            "kafka.bootstrap.servers": "localhost:9092,localhost:9092",
            "subscribe": topic,
        }

        # Read kafka stream with given parameters
        kafka_stream = (
            self.spark.readStream.format("kafka").options(**kafka_params).load()
        )

        # Convert stream into dataframe
        dataframe = (
            kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .select("key", from_json(col("value"), self.schema).alias("data"))
            .select("key", "data.*")
        )

        return dataframe

    def monitor_temperature(self, df, max_temp):
        """
        Notify user when temperature exceeds max_temp
        """
        event_time_column = "readable_time"

        # Keep data until
        delay = "10 minutes"

        temperature_monitor = (
            df.withWatermark(event_time_column, delay)
            .select("*")
            .where(df.temperature > max_temp)
        )

        return temperature_monitor

    def process_event_data(self, df, window_size: str, slide: str | None = None):
        """
        Calculate average values of columns over time window
        """

        cols = [
            "acceleration",
            "acceleration_x",
            "acceleration_y",
            "acceleration_z",
            "battery",
            "humidity",
            "pressure",
            "temperature",
        ]
        event_time_column = "readable_time"

        # Calculate column averages over time windows
        windowed = (
            df.withWatermark(event_time_column, window_size)
            .groupBy(window(event_time_column, window_size, slide))
            .agg(*[avg(col).alias(f"avg_{col}") for col in cols])
        )

        # Transform column 'window' to 'start' time and 'end' time
        windowed = windowed.select(
            windowed.window.start.cast("string").alias("start"),
            windowed.window.end.cast("string").alias("end"),
            *[f"avg_{col}" for col in cols],
        )

        return windowed

    def write_df_to_mongo(self, df_stream, collection, mode: str = "append"):
        """
        Store the dataframe stream into a MongoDB collection
        """

        (
            df_stream.writeStream.outputMode(mode)
            .format("mongodb")
            .option("checkpointLocation", f"./tmp/pyspark/{collection}")
            .option("forceDeleteTempCheckpointLocation", "true")
            .option("spark.mongodb.connection.uri", self.MONGO_URI)
            .option("spark.mongodb.database", self.name)
            .option("spark.mongodb.collection", collection)
            .start()
        )

    def mongo_batch():
        pass

    def write_df_to_kafka(self, df, topic):
        """
        Write streaming dataframe to kafka
        """

        kafka_params = {
            "kafka.bootstrap.servers": "localhost:9092,localhost:9092",
            "topic": topic,
        }

        # Transform rows into JSON, flattern to single column
        json_df = df.select(to_json(struct(df.columns)).alias("value"))

        (
            json_df.selectExpr("CAST(value AS STRING)")
            .writeStream.format("kafka")
            .options(**kafka_params)
            .option("checkpointLocation", "./tmp/pyspark/kafka")
            .option("forceDeleteTempCheckpointLocation", "true")
            .start()
        )

    def send_results(
        self, df, mongo: bool, mongo_col: str, kafka: bool, kafka_topic: str
    ):
        """
        Write df to mongo and/or kafka.
        """
        if mongo:
            self.write_df_to_mongo(df, mongo_col)
        if kafka:
            self.write_df_to_kafka(df, kafka_topic)

    def print_df(self, df_stream, mode: str = "append"):
        """Output the dataframe to console"""
        df_stream.writeStream.outputMode(mode).format("console").start()

    def await_termination(self, timeout: int | None = None):
        """
        Await stream termination, alternatively terminate when timeout seconds have elapsed
        """
        self.spark.streams.awaitAnyTermination(timeout)

    def terminate_streams(self):
        """
        Terminate all active data streams
        """
        for s in self.spark.streams.active:
            s.stop()
            return

    def close(self):
        """
        Close the sparksession gracefully
        """
        self.spark.stop()


def main():
    app = StreamApp("tenant1")
    df = app.key_stream("tenant1_queue")
    w = app.process_event_data(df, "10 minutes")
    app.print_df(w)
    app.await_termination()


if __name__ == "__main__":
    main()
