#!/usr/bin/env python
# coding: utf-8


from IPython.core.display import HTML
display(HTML("<style>pre { white-space: pre !important; }</style>"))
from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
import re


def create_spark_session():
    """Create and configure SparkSession with necessary jars."""
    spark_jars = [
        os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
        os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.5.1-sources.jar",
        os.getcwd() + "/jars/kafka-clients-3.5.1-sources.jar",
        os.getcwd() + "/jars/kafka-clients-3.5.1.jar",
        os.getcwd() + "/jars/spark-streaming_2.12-3.5.1.jar",
        os.getcwd() + "/jars/commons-pool2-2.12.0.jar",
        os.getcwd() + "/jars/spark-streaming-kafka-0-10-assembly_2.12-3.5.1.jar"
    ]
    spark_jars_path = ":".join(spark_jars)

    return SparkSession.builder \
                       .config("spark.jars", ",".join(spark_jars)) \
                       .config("spark.executor.extraClassPath", spark_jars_path) \
                       .config("spark.executor.extraLibrary", spark_jars_path) \
                       .config("spark.driver.extraClassPath", spark_jars_path) \
                       .appName("PySpark Streaming with Kafka") \
                       .getOrCreate()


def process_kafka_data(spark, kafka_topic, kafka_bootstrap_servers):
    """Read from Kafka, process data, and write to output."""
    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("subscribe", kafka_topic)
                .option("startingOffsets", "earliest")
                .load())

    df = kafka_df.selectExpr("CAST(value as STRING) as log", "timestamp")

    pattern = r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - (\d+) \[(.*?)\] (\w+) ([^\s]+) (\d+) (\d+)"
    df_with_columns = df.withColumn("ip", regexp_extract(col("log"), pattern, 1)) \
                       .withColumn("uid", regexp_extract(col("log"), pattern, 2)) \
                       .withColumn("dateTime", regexp_extract(col("log"), pattern, 3)) \
                       .withColumn("method", regexp_extract(col("log"), pattern, 4)) \
                       .withColumn("filename", regexp_extract(col("log"), pattern, 5)) \
                       .withColumn("statusCode", regexp_extract(col("log"), pattern, 6)) \
                       .withColumn("fileSize", regexp_extract(col("log"), pattern, 7))

    windowedCounts = df_with_columns \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes")) \
        .agg(
            count(when((col("method") == "POST") & (col("statusCode") == "200"), True)).alias("no_of_successful_POST_operations"),
            count(when((col("method") == "POST") & (col("statusCode") > "200"), True)).alias("no_of_failed_POST_operations"),
            count(when((col("method") == "GET") & (col("statusCode") == "200"), True)).alias("no_of_successful_GET_operations"),
            count(when((col("method") == "GET") & (col("statusCode") > "200"), True)).alias("no_of_failed_GET_operations")
        )

    return windowedCounts

def start_streaming_query(query, output_path, checkpoint_path):
    """Start the streaming query."""
    query.writeStream \
         .outputMode("append") \
         .format("json") \
         .option("path", output_path) \
         .option("checkpointLocation", checkpoint_path) \
         .option("truncate", "false") \
         .trigger(processingTime="5 minute") \
         .start()



def main():
    """Main function to orchestrate the streaming application."""
    spark = create_spark_session()

    KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC2")
    KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

    windowedCounts = process_kafka_data(spark, KAFKA_TOPIC_NAME, KAFKA_BOOTSTRAP_SERVER)

    output_path = "hdfs://localhost:8020/load-balancer-logs/aggregated_stream_logs"
    checkpoint_path = "hdfs://localhost:8020/user/hdfs/checkpoint"

    query = start_streaming_query(windowedCounts, output_path, checkpoint_path)


if __name__ == "__main__":
    main()