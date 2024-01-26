import random
import time
import json
import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook
import openpyxl
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Настройка аргументов DAG
default_args = {
    'owner': 'GeorgeYA',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}   

def scrapy():
    processed_links = 0

    # Создание нового XLSX файла
    workbook = openpyxl.Workbook()
    worksheet = workbook.active

    # Заголовки столбцов
    worksheet.append(["id", "price", "title", "handle"])


    with open('/mnt/c/Users/dags_airflow/dags/ids.txt', 'r') as id_list:
        for i_id in id_list:
            i_id = i_id.strip()
            print(i_id)
            time.sleep(random.uniform(1, 2))

            url = f"https://super-export-shop.myshopify.com/admin/api/2023-10/products/{i_id}.json"
            auth = ("3a1e6b71fda6c628e51a059c796fe501", "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")

            response = requests.get(url, auth=auth)

            if response.status_code == 200:
                data = response.json()

                product_id = data['product']['id']
                product_price = data['product']['variants'][0]['price']
                product_title = data['product']['title']
                product_handle = "https://superexportshop.org/products/" + data['product']['handle']

                worksheet.append([product_id, product_price, product_title, product_handle])
                print(product_price)
                processed_links += 1


            else:
                workbook.save(f"/mnt/c/Users/dags_airflow/dags/products_super_export.xlsx")
                print(f"Ошибка {response.status_code}: Не удалось выполнить запрос."
                    f"Данные успешно сохранены в XLSX файл")

    # Сохранение файла
    workbook.save(f"/mnt/c/Users/dags_airflow/dags/products_super_export.xlsx")
    print(f'\nПойдено {processed_links} ссылок. Данные успешно сохранены в XLSX файл')

    return {
            'processed_links': int(processed_links),
        }

# Создание объекта DAG
with DAG(
    dag_id='super_export_parser_pilot', # Имя DAG
    description='Тестовый запуск',
    default_args=default_args,
    start_date=datetime(2023, 11, 1, 2),
    schedule_interval='@daily'
    #catchup=False,  # Отключить выполнение отставания
) as dag:
    
    # Создание PythonOperator, который выполнит парсер
    parse_task = PythonOperator(
        task_id='parse_task_super_export',
        python_callable=scrapy,
    )

    # Порядок выполнения задач
    parse_task