from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "start_date": datetime.now(datetime.timezone.utc),
    "retries": 1,
}

with DAG(
    dag_id="silver_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_silver_pipeline",
        bash_command="""
        docker exec -it spark /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 --conf spark.jars.ivy=/tmp/.ivy2 pipelines/silver_transform_pipeline.py
        """
    )

    run_spark_job