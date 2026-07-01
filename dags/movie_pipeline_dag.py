import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    description='Автономный пайплайн обработки отзывов',
    schedule_interval='@daily',
    catchup=False,
)

def extract_local_data(**kwargs):
    print("Генерация текстовых отзывов на основе локальных шаблонов...")
    templates = [
        "Amazing story and great acting!",
        "Too long, I fell asleep in the middle.",
        "A masterpiece of modern cinema.",
        "Not worth your time, very predictable plot.",
        "Visual effects were stunning, love it!"
    ]
    
    processed_data = []
    for i in range(1, 11):
        movie_id = random.randint(100, 999)
        review_text = random.choice(templates)
        rating = round(random.uniform(5.0, 10.0), 1)
        processed_data.append((movie_id, review_text, rating))
        
    kwargs['ti'].xcom_push(key='reviews_data', value=processed_data)

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='reviews_data', task_ids='extract_data')
    
    import psycopg2
    conn = psycopg2.connect(
        host='postgres', database='movies_db', user='airflow', password='airflow', port='5432'
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_reviews (
            id SERIAL PRIMARY KEY,
            movie_id INT,
            review_text VARCHAR(255),
            calculated_rating NUMERIC(3,1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    insert_query = "INSERT INTO movie_reviews (movie_id, review_text, calculated_rating) VALUES (%s, %s, %s)"
    cursor.executemany(insert_query, data)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Данные успешно сохранены в таблицу movie_reviews!")

task_extract = PythonOperator(task_id='extract_data', python_callable=extract_local_data, dag=dag)
task_transform_load = PythonOperator(task_id='transform_and_load', python_callable=load_data_to_postgres, dag=dag)

task_extract >> task_transform_load
