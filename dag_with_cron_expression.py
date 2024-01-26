from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'GeorgeYA',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_cron_expression_v03',
    default_args=default_args,
    description='Планирование выполнения DAG-файлов на определённое время(выраженный Cron`ом).', # Airflow Scheduler with Cron Expression
    start_date=datetime(2023, 10, 25),
    #schedule_interval='@daily'
    schedule_interval='0 3 * * Tue'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo DAG выраженный Cron!'
    )
    task1