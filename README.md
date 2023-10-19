# AIS-data-pipeline
End-to-End Automated Data Pipeline: From Data Acquisition to Visualization with Dockerized Spark, Kafka, HDFS, and Airflow
**Data Engineering Project Overview**

**Objective**:
Build a system that automates data download, processes it, and prepares it for visualization.

**Components**:
1. **Docker**: Ensures our setup works uniformly across different environments.
2. **Apache Spark**: Handles large-scale data processing.
3. **HDFS**: A place to store our large datasets.
4. **Kafka**: Manages real-time data feeds.
5. **PostgreSQL**: A database for storing our processed data.
6. **Metabase**: A tool to visualize our data.
7. **Airflow**: Schedules and automates our tasks.

**Workflow**:

1. **Setup**:
   - Use Docker to create a system with Spark, Hadoop, Kafka, PostgreSQL, Metabase, and other necessary tools.
   
2. **Data Collection and Storage**:
   - Automatically download data from a specific website.
   - Store the data in HDFS.
   
3. **Task Automation**:
   - Use Airflow to schedule daily data downloads and processing between specific dates.
   
4. **Data Transformation**:
   - Read the stored data, clean it, and gather insights on popular destinations.
   
**Outcome**:
The system will automatically gather, process, and store data daily, making it ready for visual analysis.

**Recommendations**:
1. Add error-handling to ensure smooth operations.
2. Push the results to PostgreSQL for easy visualization.
3. Monitor the system's health regularly.
