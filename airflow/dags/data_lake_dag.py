from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from python.src.football_data_service import extract_data_and_load_to_gs

def hello():
    print("hello world")


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
        python_callable=extract_data_and_load_to_gs
    )

    task1
