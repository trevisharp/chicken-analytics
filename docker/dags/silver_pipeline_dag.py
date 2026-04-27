import os
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "start_date": datetime.now(datetime.timezone.utc),
    "retries": 1,
}

def bronze_updated(**context):
    BRONZE_PATH = "opt/airflow/data/bronze"
    files = [
        os.path.join(BRONZE_PATH, f)
        for f in os.listdir(BRONZE_PATH)
    ]

    if not files:
        return False

    latest_mod = max(os.path.getmtime(f) for f in files)

    last_run = context['ti'].xcom_pull(
        key='last_processed_time',
        task_ids='check_bronze'
    )

    if last_run is None or latest_mod > last_run:
        context['ti'].xcom_push(
            key='last_processed_time',
            value=latest_mod
        )
        return True

    return False

with DAG(
    dag_id="silver_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    wait_for_bronze = PythonSensor(
        task_id="check_bronze",
        python_callable=bronze_updated,
        poke_interval=60,
        mode="poke"
    )

    run_spark_job = BashOperator(
        task_id="run_silver_pipeline",
        bash_command="""
        docker exec -it spark /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 --conf spark.jars.ivy=/tmp/.ivy2 pipelines/silver_transform_pipeline.py
        """
    )

    wait_for_bronze >> run_spark_job