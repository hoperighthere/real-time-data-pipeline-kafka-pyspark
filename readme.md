# Servers Metrics & Logs
## Project Overview
This project involves setting up a multi-node Kafka cluster to handle metrics and logs from a cluster of 10 servers and a load balancer hosting a cloud storage website. The metrics from the 10 servers are consumed and stored in a relational database, while the logs from the load balancer are processed using a Spark application to compute a moving window count of operations every 5 minutes and store the results in a Hadoop system.

## Project Structure
+ Kafka Cluster: Multi-node Kafka cluster with two topics: one for server metrics and another for load balancer logs.
+ Java Agents: Simulate agents sending data to the Kafka topics.
+ Python Consumer: Consumes metrics from Kafka and inserts them into PostgreSQL.
+ Spark Application: Consumes logs from Kafka, processes them, and stores the results in HDFS

## Prerequisites
+ Docker (for PostgreSQL)
+ Maven (for Java agents)
+ Hadoop (HDFS)
+ Apache Spark
+ Python (with psycopg2, pyspark libraries)
+ Kafka (multi-node setup)

