import random
import time
import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook
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
    wb = Workbook()
    ws = wb.active
    ws.append(["Product Name", "Price", "Product Link"])

    zero_price_products = [] 
    processed_links = 0 

    with open('/mnt/c/Users/dags_airflow/dags/urls.txt', 'r') as urls:
        for url in urls:
            url = url.strip() 
            print(url)
            time.sleep(random.uniform(3, 5))
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                product_name = soup.find('h1',
                                         class_="product-area__details__title product-detail__gap-sm h2").text.strip()
                price = soup.find('div', class_="price-area product-detail__gap-sm ikii")
                product_price = price.text.strip()[1:] if price is not None else "0"

                product_info = [product_name, product_price, url]
                print(product_info)
                ws.append(product_info)
                processed_links += 1
                wb.save('/mnt/c/Users/dags_airflow/dags/product_info_catworkwear.xlsx')

                if product_price == "0":
                    zero_price_products.append(url)
            else:
                print(f'Ошибка при запросе к веб-сайту {url}. Код состояния: {response.status_code}')

    print(f'\nПойдено {processed_links} ссылок.')
    if zero_price_products:
        print(f'\nПродукты с ценой 0 в количестве {len(zero_price_products)} штук записаны в фаил '
              f'"/mnt/c/Users/dags_airflow/dags/zero_price_products.txt"')
        with open('/mnt/c/Users/dags_airflow/dags/zero_price_products.txt', 'w') as file:
            for product_url_zero in zero_price_products:
                file.write(f"{product_url_zero}\n")
    return {
            'processed_links': int(processed_links),
            'zero_price_products': len(zero_price_products)
        }

# Создание объекта DAG
with DAG(
    dag_id='catworkwear_parser_dag_pilot', # Имя DAG
    description='Тестовый запуск',
    default_args=default_args,
    start_date=datetime(2023, 11, 1, 2),
    schedule_interval='@daily'
    #catchup=False,  # Отключить выполнение отставания
) as dag:
    
    # Создание PythonOperator, который выполнит парсер
    parse_task = PythonOperator(
        task_id='parse_task_catworkwear',
        python_callable=scrapy,
    )

    # Порядок выполнения задач
    parse_task