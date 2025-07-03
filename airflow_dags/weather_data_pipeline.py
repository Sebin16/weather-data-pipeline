# DAG: weather_data_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='Run weather data scripts in sequence and parallel using Airflow',
    schedule='*/5 * * * *',
    catchup=False
)

run_producer = BashOperator(
    task_id='run_producer_script',
    bash_command='mkdir -p /home/sebin/ProjectAPI/logs && /home/sebin/ProjectAPI/venv/bin/python /home/sebin/ProjectAPI/producer1.py >> /home/sebin/ProjectAPI/logs/producer_log.txt 2>&1',
    dag=dag
)

run_spark_stream = BashOperator(
    task_id='run_spark_consumer',
    bash_command="""
/home/sebin/spark/spark-3.5.4-bin-hadoop3/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
/home/sebin/ProjectAPI/spark_consumer.py >> /home/sebin/ProjectAPI/logs/spark_log.txt 2>&1
""",
    dag=dag
)

run_kafka_consumer = BashOperator(
    task_id='run_consumer_script',
    bash_command='/home/sebin/ProjectAPI/venv/bin/python /home/sebin/ProjectAPI/consumer1.py '
                 '>> /home/sebin/ProjectAPI/logs/consumer_log.txt 2>&1',
    dag=dag
)

run_producer >> [run_spark_stream, run_kafka_consumer]
