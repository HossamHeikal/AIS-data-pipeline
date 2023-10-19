# airflow_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing your functions from download.py
from download import download_file, upload_to_hdfs, send_to_kafka_and_spark

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'end_date': datetime(2023, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'download_and_process_files',
    default_args=default_args,
    description='Download, process and upload files',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Create the Airflow tasks
def download_task(**kwargs):
    date = kwargs['ds']
    url = f"http://web.ais.dk/aisdata/aisdk-{date}.zip"
    download_file(url)

def upload_task(**kwargs):
    date = kwargs['ds']
    file_name = f"aisdk-{date}.zip"
    upload_to_hdfs(file_name)

def send_task(**kwargs):
    date = kwargs['ds']
    url = f"http://web.ais.dk/aisdata/aisdk-{date}.zip"
    send_to_kafka_and_spark(url)

t1 = PythonOperator(
    task_id='download_file',
    python_callable=download_task,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='upload_to_hdfs',
    python_callable=upload_task,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='send_to_kafka_and_spark',
    python_callable=send_task,
    provide_context=True,
    dag=dag
)

# Set the task execution order
t1 >> t2 >> t3
