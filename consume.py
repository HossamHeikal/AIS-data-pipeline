from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from kafka import KafkaConsumer
import json

# Create a Kafka consumer using kafka-python
consumer = KafkaConsumer(
    'parquet_file',
    bootstrap_servers='kafka1:29092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Create a Spark session
spark = SparkSession.builder.appName("Consume Kafka Events and aggregate Parquet").master("spark://spark-master:7077").config("spark.executor.cores", "4").config("spark.executor.memory", "1g").getOrCreate()

for message in consumer:
    parquet_path = message.value["parquet_path"]
    data = spark.read.parquet(parquet_path)

    # Convert to Parquet and write to HDFS
    clean_data = data.filter(col("Destination").isNotNull() & (col("Destination") != ""))

# Group by destination and count occurrences
    popular_destinations = (
        clean_data.groupBy("Destination")
        .agg(count("*").alias("Count"))
        .orderBy("Count", ascending=False)
    )
    popular_destinations = popular_destinations.toDF(*[c.lower() for c in popular_destinations.columns])
# Save the popular destinations analysis back to HDFS
    popular_destinations.write.mode("overwrite").parquet("hdfs://namenode:8020/aggs/PDA.parquet")

spark.stop()