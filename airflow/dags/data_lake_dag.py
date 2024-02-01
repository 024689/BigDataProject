from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import sys

sys.path.insert(0, '/home/juniortemgoua0/DataLake/')
from jobs.extractions.python.sources.football_data.football_data_service import extract_data_and_load_to_gs

project_id = 'data-lake-project-409321'
region = 'europe-west9'
cluster_name = 'cluster-f499'
hdfs_datalake_path = "hdfs://34.155.106.39:8020/datalake"
gcs_datalake_path = "gs://data-lake-buck/datalake"


def end_day_airflow_processes(**kwargs):
    print("***************************************")
    print("DAG processes execute successfully \n")
    print(f"Date: {kwargs['ds']}")
    print("***************************************")


default_args = {
    "owner": "juniortemgoua0",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
        dag_id="manage_data_lake_dag",
        description="This is dag to manage the data lake",
        schedule_interval="@daily",
        start_date=datetime(2024, 0o1, 0o1),
        default_args=default_args
) as dag:
    pass
    task1 = PythonOperator(
        task_id="extract_data_and_load_to_gs",
        python_callable=extract_data_and_load_to_gs,
        provide_context=True
    )

    task2 = SparkSubmitOperator(
        task_id="local_job_raw_to_formatted",
        conn_id='spark_default',
        application="/home/juniortemgoua0/DataLake/jobs/processes/scala/spark_process/target/scala-2.12/spark_job_2"
                    ".12-0.1.0.jar",
        java_class="RawToFormatted",
        application_args=[hdfs_datalake_path, "local-spark"],
        conf={
            'spark.airflow.execution_date': '{{ ds }}'
        },
    )

    task3 = SparkSubmitOperator(
        task_id="local_job_formatted_to_usage",
        conn_id='spark_default',
        application="/home/juniortemgoua0/DataLake/jobs/processes/scala/spark_process/target/scala-2.12/spark_job_2"
                    ".12-0.1.0.jar",
        java_class="FormattedToUsage",
        application_args=[hdfs_datalake_path, "local-spark"],
        conf={
            'spark.airflow.execution_date': '{{ ds }}'
        },
    )

    task4 = DataprocSubmitJobOperator(
        task_id='dataproc_job_raw_to_formatted',
        gcp_conn_id="google_cloud_default",
        project_id=project_id,
        job={
            "reference": {
                "project_id": project_id,
            },
            "placement": {
                "cluster_name": cluster_name,
            },
            "spark_job": {
                "args": [
                    gcs_datalake_path,
                    "dataproc"
                ],
                "jar_file_uris": [
                    "gs://data-lake-buck/spark_job_2.12-0.1.0.jar"
                ],
                "properties": {
                    'spark.airflow.execution_date': '{{ ds }}'
                },
                "main_class": "RawToFormatted"
            },
        },
        region=region,
        executor_config={'spark.airflow.execution_date': '{{ ds }}'}
    )

    task5 = DataprocSubmitJobOperator(
        task_id='dataproc_job_formatted_to_usage',
        gcp_conn_id="google_cloud_default",
        project_id=project_id,
        job={
            "reference": {
                "project_id": project_id,
            },
            "placement": {
                "cluster_name": cluster_name,
            },
            "spark_job": {
                "args": [
                    gcs_datalake_path,
                    "dataproc"
                ],
                "jar_file_uris": [
                    "gs://data-lake-buck/spark_job_2.12-0.1.0.jar"
                ],
                "properties": {
                    'spark.airflow.execution_date': '{{ ds }}'
                },
                "main_class": "FormattedToUsage"
            },
        },
        region=region,
        executor_config={'spark.airflow.execution_date': '{{ ds }}'}
    )

    task6 = PythonOperator(
        task_id="end_airflow_process",
        python_callable=extract_data_and_load_to_gs,
        provide_context=True
    )

    task1 >> task2 >> task3 >> task6
    task1 >> task4 >> task5 >> task6
