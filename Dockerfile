FROM bitnami/spark:latest

# Copy the python scripts to the image
COPY postgresql-42.6.0.jar /opt/bitnami/spark/jars/
COPY download.py /opt/download.py
COPY consume.py /opt/consume.py
# Install kafka-python using pip
RUN pip install wheel
RUN pip install kafka-python 
RUN pip install hdfs 
RUN pip install tqdm
RUN pip install apache-airflow
#RUN pip install psycopg2
RUN pip install psycopg2-binary

