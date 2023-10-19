# download.py
import os
import requests
from tqdm import tqdm
from zipfile import ZipFile
from hdfs import InsecureClient
from kafka import KafkaProducer
import json
from pyspark.sql import SparkSession

def download_file(url):
    response = requests.get(url, stream=True)
    file_size = int(response.headers['Content-Length'])
    chunk_size = 1024  # 1 KB
    file_name = url.split("/")[-1]

    with open(file_name, "wb") as file:
        for chunk in tqdm(response.iter_content(chunk_size=chunk_size), total=file_size//chunk_size, unit="KB"):
            file.write(chunk)
    return file_name

def upload_to_hdfs(file_name):
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    hdfs_path = '/data/' + file_name

    with open(file_name, "rb") as file:
        hdfs_client.write(hdfs_path, file)

    unzip_dir = 'unzipped_data'
    with ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)

    for root, dirs, files in os.walk(unzip_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            hdfs_file_path = '/data/unzipped/' + file
            with open(local_file_path, 'rb') as local_file:
                hdfs_client.write(hdfs_file_path, local_file)

def send_to_kafka_and_spark(url):
    producer = KafkaProducer(bootstrap_servers='kafka1:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    date_string = url.split('/')[-1].replace('.zip', '')
    csv_path_on_hdfs = '/data/unzipped/' + date_string + '.csv'

    spark = SparkSession.builder.appName("convert from CSV to Parquet").master("spark://spark-master:7077").config("spark.executor.cores", "4").config("spark.executor.memory", "1g").getOrCreate()
    df = spark.read.csv(csv_path_on_hdfs, header=True, inferSchema=True)
    parquet_path = f"hdfs://namenode:8020/parquet/"  + date_string + ".parquet"
    df.write.parquet(parquet_path)

    producer.send('parquet_file', {'parquet_path': parquet_path})
    spark.stop()