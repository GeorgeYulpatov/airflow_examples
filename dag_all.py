from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


# Импорт два DAG файла
from catworkwear_parser_dag_pilot import catworkwear_parser_dag_pilot as dag1
from super_export_parser_pilot import super_export_parser_pilot as dag2

# Настройка аргументов общего DAG
default_args = {
    'owner': 'GeorgeYA',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 3, 2),
}

# Создайте общий DAG
dag = DAG(
    dag_id='common_dag', # Имя DAG
    description='Тестовый запуск паралельный',
    default_args=default_args,
    start_date=datetime(2023, 11, 3, 2),
    catchup=False,  # Отключите выполнение отставания
)

# Создайте операторы DummyOperator для ожидания
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Создайте операторы SubDagOperator для запуска ваших DAG файлов как поддаги
subdag1 = SubDagOperator(
    task_id='run_dag1',
    subdag=dag1,
    dag=dag,
)
subdag2 = SubDagOperator(
    task_id='run_dag2',
    subdag=dag2,
    dag=dag,
)

# Определите порядок выполнения операторов
start >> [subdag1, subdag2] >> end