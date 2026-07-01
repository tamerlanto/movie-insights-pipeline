import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'movie_insights_pipeline',
    default_args=default_args,
    description='Пайплайн обработки отзывов с загрузкой в ClickHouse',
    schedule_interval='@daily',
    catchup=False,
)

def extract_local_data(**kwargs):
    print("Генерация отзывов для ClickHouse...")
    templates = [
        "Amazing story and great acting!",
        "Too long, I fell asleep in the middle.",
        "A masterpiece of modern cinema.",
        "Not worth your time, very predictable plot.",
        "Visual effects were stunning, love it!"
    ]
    
    processed_data = []
    # Сгенерируем 15 записей для проверки
    for _ in range(15):
        movie_id = random.randint(100, 999)
        review_text = random.choice(templates)
        rating = round(random.uniform(5.0, 10.0), 1)
        processed_data.append((movie_id, review_text, rating))
        
    kwargs['ti'].xcom_push(key='reviews_data', value=processed_data)

def load_data_to_clickhouse(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='reviews_data', task_ids='extract_data')
    
    print("Подключение к ClickHouse-серверу...")
    # Подключаемся к контейнеру напрямую по порту 9000 внутри Docker-сети
    client = Client(
        host='clickhouse',
        port=9000,
        user='developer',
        password='clickhouse_pass',
        database='movies_analytics'
    )
    
    print("Создание таблицы на движке MergeTree...")
    # В ClickHouse обязательно указывать сортировку ORDER BY для движков MergeTree
    client.execute("""
        CREATE TABLE IF NOT EXISTS ch_movie_reviews (
            movie_id Int32,
            review_text String,
            calculated_rating Float32,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY movie_id;
    """)
    
    print("Выполняем пакетную вставку в ClickHouse...")
    client.execute(
        "INSERT INTO ch_movie_reviews (movie_id, review_text, calculated_rating) VALUES",
        data
    )
    print(f"Успешно сохранено {len(data)} записей в ClickHouse!")

task_extract = PythonOperator(task_id='extract_data', python_callable=extract_local_data, dag=dag)
task_transform_load = PythonOperator(task_id='transform_and_load', python_callable=load_data_to_clickhouse, dag=dag)

task_extract >> task_transform_load
