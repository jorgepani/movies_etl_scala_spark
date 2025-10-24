from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="movies_etl_scala_spark",
    start_date=datetime(2025,10,1),
    schedule="@daily",
    catchup=False,
) as dag:
    spark_job = SparkSubmitOperator(
        task_id="spark_etl_scala",
        application="/opt/jars/movies-etl_2.12-0.1.0.jar",
        java_class="movies.etl.Main",
        application_args=["--input","/opt/data","--output","/opt/out"],
        conf={"spark.sql.shuffle.partitions":"200"},
    )
