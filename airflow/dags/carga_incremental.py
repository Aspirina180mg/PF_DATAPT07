from airflow import DAG
from airflow.decorators import task
from airflow.hooks.http_hook import HttpHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

FILES = {
    'sites_google': 'sites_google_2020_01_01.json',
    'sites_google_reviews': 'sites_google_reviews_2020_01_01.json',
    'sites_yelp': 'sites_yelp_2020_01_01.json',
    'sites_yelp_reviews': 'sites_yelp_reviews_2020_01_01.json',
}

# Helper function to fetch and save data
def fetch_and_save_data(endpoint, output_path):
    http_hook = HttpHook(http_conn_id='http_localhost', method='GET')
    response = http_hook.run(endpoint)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data['data'] if 'data' in data else data)
    df.to_json(output_path, index=False, orient='records')

# Task to fetch sites data
@task
def sites_google_api():
    output_path = f"s3/ptf-bucket/{FILES['sites_google']}"
    fetch_and_save_data("sites_google", output_path)

# Task to fetch sites reviews data
@task
def sites_google_reviews_api():
    sites_google = 'sites_google_reviews'
    output_path = f's3/ptf-bucket/{FILES[sites_google]}'
    fetch_and_save_data('sites_google_reviews/2021/1/1', output_path)

# Task to fetch Yelp sites data
@task
def sites_yelp_api():
    sites_yelp = 'sites_yelp'
    output_path = f's3/ptf-bucket/{FILES[sites_yelp]}'
    fetch_and_save_data("sites_yelp", output_path)

# Task to fetch Yelp reviews data
@task
def sites_yelp_reviews_api():
    sites_yelp_reviews = 'sites_yelp_reviews'
    output_path = f's3/ptf-bucket/{FILES[sites_yelp_reviews]}'
    fetch_and_save_data("sites_yelp_reviews/2020/1/1", output_path)

with DAG(
    'carga_incremental',
    default_args=default_args,
    description='Consulta a las fuentes de datos automatizadas',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    sites_google_api_task = sites_google_api()
    sites_google_reviews_task = sites_google_reviews_api()
    
    sites_yelp_api_task = sites_yelp_api()
    sites_yelp_reviews_api_task = sites_yelp_reviews_api()

    sites_google_api_task >> sites_google_reviews_task 
    sites_yelp_api_task >> sites_yelp_reviews_api_task
