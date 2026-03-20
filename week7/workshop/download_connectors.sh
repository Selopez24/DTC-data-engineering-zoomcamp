#!/bin/bash
cd "$(dirname "$0")"

echo "Downloading Flink connectors..."
mkdir -p lib

FLINK_VERSION=1.18.1
SCALA_VERSION=2.12

curl -fsSL -o lib/flink-connector-kafka-3.0.1-1.18.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.18/flink-connector-kafka-3.0.1-1.18.jar"

curl -fsSL -o lib/flink-sql-connector-kafka-3.0.1-1.18.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"

curl -fsSL -o lib/flink-connector-jdbc-3.1.0-1.18.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.0-1.18/flink-connector-jdbc-3.1.0-1.18.jar"

curl -fsSL -o lib/postgresql-42.7.1.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar"

curl -fsSL -o lib/flink-json-1.18.1.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar"

echo "Done!"
