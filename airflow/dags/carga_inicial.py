from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
from master import ETL, CargaInicial

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
import json

etl = CargaInicial()


def extract_sites():
    print('Extrayendo información de Sitios')
    df_restaurantes = pd.read_parquet('data/generated/restaurantes.parquet')
    top_20 = df_restaurantes['nombre'].value_counts().head(20).index
    df_restaurantes_filtrado = df_restaurantes[df_restaurantes['nombre'].isin(top_20[1:])]
    df_restaurantes_filtrado['atributos'] = df_restaurantes_filtrado['atributos'].astype(str)

    DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-postgres:5432/warehouse'

    engine = create_engine(DATABASE_URI, convert_unicode=True)

    stmt = engine.connect()
    metadata = MetaData()
    table = Table('restaurantes', metadata, autoload_with=engine)

    print("Comenzando inserción de datos en DW...")
    errors = 0
    intentos = 0

    for i, v in df_restaurantes_filtrado.iterrows():    
        intentos += 1
        linea = v
        insert_stmt = table.insert().values(linea)
        try:
            with stmt.begin():
                stmt.execute(insert_stmt)
                try: 
                    stmt.commit()
                    insertados += 1
                except:
                    pass
        except SQLAlchemyError as e:
            # print(f"Error: {e}")
            errors += 1
            pass
    
    
    info = f'''
    La cantidad de insertados es {intentos - errors}
    La cantidad de errores es {errors}
    '''
    print(info)

def extract_reviews():
    print('Extrayendo información de Reseñas')
    df_reviews = pd.read_parquet('data/generated/reviews.parquet')
    df_reviews_filtradas = df_reviews[df_reviews['anio'].isin([2017, 2018, 2019])].sample(50000)

    DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-postgres:5432/warehouse'
    engine = create_engine(DATABASE_URI)
    stmt = engine.connect()
    metadata = MetaData()

    table = Table('reviews', metadata, autoload_with=engine)

    print("Comenzando inserción de datos en DW...")
    errors = 0
    intentos = 0

    for i, v in df_reviews_filtradas.iterrows():    
        intentos += 1
        linea = v
        insert_stmt = table.insert().values(linea)
        try:
            with stmt.begin():
                stmt.execute(insert_stmt)
                try: 
                    stmt.commit()
                    insertados += 1
                except:
                    pass
        except SQLAlchemyError as e:
            # print(f"Error: {e}")
            errors += 1
            pass
    
    info = f'''
    La cantidad de insertados es {intentos - errors}
    La cantidad de errores es {errors}
    '''
    print(info)

def extract_states():
    print('Extrayendo información Estados')
    df_estados = pd.read_parquet('data/generated/estados.parquet')

    DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-postgres:5432/warehouse'
    engine = create_engine(DATABASE_URI)
    stmt = engine.connect()
    metadata = MetaData()

    table = Table('estados', metadata, autoload_with=engine)

    print("Comenzando inserción de datos en DW...")
    errors = 0
    intentos = 0

    for i, v in df_estados.iterrows():    
        intentos += 1
        linea = v
        insert_stmt = table.insert().values(linea)
        try:
            with stmt.begin():
                stmt.execute(insert_stmt)
                try: 
                    stmt.commit()
                except:
                    pass
        except SQLAlchemyError as e:
            # print(f"Error: {e}")
            errors += 1
            pass
    
        
    info = f'''
    La cantidad de insertados es {intentos - errors}
    La cantidad de errores es {errors}
    '''
    print(info)

dag = DAG(
    'carga_inicial',
    default_args={'start_date': days_ago(1)},
    # schedule_interval='0 23 * * *',
    schedule_interval='@daily',
    catchup=False
)


etl_extract_sites = PythonOperator(
    task_id='etl_extract_sites',
    python_callable = extract_sites,
    dag=dag
)

etl_extract_reviews = PythonOperator(
    task_id='etl_extract_review',
    python_callable = extract_reviews,
    dag=dag
)

etl_extract_states = PythonOperator(
    task_id='etl_extract_states',
    python_callable = extract_states,
    dag=dag
)



etl_extract_sites >> etl_extract_reviews >> etl_extract_states

