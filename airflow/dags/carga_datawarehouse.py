from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
# from airflow.sensors.filesystem import FileSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
import json
import os

# Define the local directory and file names
LOCAL_BUCKET_PATH = 's3/ptf-bucket'

FILES = {
    'sites_google': 'sites_google_2020_01_01.json',
    'sites_google_reviews': 'sites_google_reviews_2020_01_01.json',
    'sites_yelp': 'sites_yelp_2020_01_01.json',
    'sites_yelp_reviews': 'sites_yelp_reviews_2020_01_01.json',
}

DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-postgres:5432/warehouse'

def list_files():
    files = os.listdir(LOCAL_BUCKET_PATH)
    print("Files in directory:", files)
    return files

@task
def sites_google_to_dw():
    file_path = os.path.join(LOCAL_BUCKET_PATH, FILES['sites_google'])
    table_name = "restaurantes"

    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            content = file.read()
            print(f"tamaño de dataset {len(content)}")
            data = pd.DataFrame(json.loads(content))
            data = data[['name', 'gmap_id', 'category', 'num_of_reviews', 'latitude', 'longitude', 'MISC', 'avg_rating']]
            print("renombrando datos")
            data.rename(
                columns={
                    "name": "nombre",
                    "gmap_id": "id_restaurante",
                    "category": "categorias",
                    "num_of_reviews": "cantidad_resenas",
                    "latitude": "latitud",
                    "longitude": "longitud",
                    "MISC": "atributos",
                    "avg_rating": "calificacion"}, inplace=True)
            print("creando conexion")
            # Load data into the database
            engine = create_engine(DATABASE_URI)
            print(engine)
            for column in data.columns:
                if isinstance(data[column].iloc[0], dict) or isinstance(data[column].iloc[0], list):
                    data[column] = data[column].apply(json.dumps)  
            print("cambiando tipos")
            stmt = engine.connect()

            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)

            for i, v in data.iterrows():    
                linea = v
                insert_stmt = table.insert().values(linea)
                try:
                    with stmt.begin():
                        stmt.execute(insert_stmt)
                        try: 
                            stmt.commit()
                        except:
                            pass
                            # print("no se puede comitear algo que no existe")
                except SQLAlchemyError as e:
                    # print(f"Error: {e}")
                    pass


            # print(f"salida: {out}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

@task
def sites_google_reviews_to_dw():
    file_path = os.path.join(LOCAL_BUCKET_PATH, FILES['sites_google_reviews'])
    table_name = "reviews"
    
    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            content = file.read()
            data = pd.DataFrame(json.loads(content))
            print(f"tamaño de dataset {len(content)}")
            
            # Load data into the database
            engine = create_engine(DATABASE_URI)

            for column in data.columns:
                if isinstance(data[column].iloc[0], dict) or isinstance(data[column].iloc[0], list):
                    data[column] = data[column].apply(json.dumps)  
            
            data['review_id'] = data['gmap_id'].str[:10] + data['user_id'].str[:10]
            data['sentiment_score'] = 0
            data = data[["user_id", "gmap_id", "review_id", "rating", "anio", "sentiment_score"]]

            data = data.rename(
            columns={
                "user_id": "id_usuario",
                "gmap_id": "id_restaurante",
                "review_id": "id_resena",
                "rating": "calificacion",
                "anio": "anio",
                "sentiment_score": "puntaje_de_sentimiento",})

            stmt = engine.connect()

            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)

            for i, v in data.iterrows():    
                linea = v
                insert_stmt = table.insert().values(linea)
                try:
                    with stmt.begin():
                        stmt.execute(insert_stmt)
                        try: 
                            stmt.commit()
                        except:
                            pass
                            # print("no se puede comitear algo que no existe")
                except SQLAlchemyError as e:
                    # print(f"Error: {e}")
                    pass


            # out = data.to_sql(table_name, con=engine, if_exists='append', index=False)
            # print(f"salida: {out}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

@task
def sites_yelp_to_dw():
    file_path = os.path.join(LOCAL_BUCKET_PATH, FILES['sites_yelp'])
    table_name = "restaurantes"

    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            content = file.read()
            data = pd.DataFrame(json.loads(content))
            print(f"tamaño de dataset {len(content)}")
            
            # Load data into the database
            engine = create_engine(DATABASE_URI)
        
        data = data[["name", "business_id", "categories", "review_count", "latitude", "longitude", "attributes", "stars"]]
        data['categories'] = data['categories'].apply(json.dumps)
        data['attributes'] = data['attributes'].apply(json.dumps)

        data = data.rename(
            columns={
                "name": "nombre",
                "business_id": "id_restaurante",
                "categories": "categorias",
                "review_count": "cantidad_resenas",
                "latitude": "latitud",
                "longitude": "longitud",
                "attributes": "atributos",
                "stars": "calificacion"})

        stmt = engine.connect()

        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)

        for i, v in data.iterrows():    
            linea = v
            insert_stmt = table.insert().values(linea)
            try:
                with stmt.begin():
                    stmt.execute(insert_stmt)
                    try: 
                        stmt.commit()
                    except:
                        pass
                        # print("no se puede comitear algo que no existe")
            except SQLAlchemyError as e:
                # print(f"Error: {e}")
                pass
        
        # out = data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        # print(f"salida: {out}")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

@task
def sites_yelp_reviews_to_dw():
    file_path = os.path.join(LOCAL_BUCKET_PATH, FILES['sites_yelp_reviews'])
    table_name = "reviews"
    
    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            content = file.read()
            data = pd.DataFrame(json.loads(content))
            print(f"tamaño de dataset {len(content)}")
            
            # Load data into the database
            engine = create_engine(DATABASE_URI)
        
            data["anio"] = data["date"].str[:4]
            data['sentiment_score'] = 0
            data = data[["user_id","business_id","review_id","stars","anio","sentiment_score",]]
            data = data.rename(
                columns={
                    "user_id": "id_usuario",
                    "business_id": "id_restaurante",
                    "review_id": "id_resena",
                    "stars": "calificacion",
                    "anio": "anio",
                    "sentiment_score": "puntaje_de_sentimiento",})

        stmt = engine.connect()

        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)

        for i, v in data.iterrows():    
            linea = v
            insert_stmt = table.insert().values(linea)
            try:
                with stmt.begin():
                    stmt.execute(insert_stmt)
                    try: 
                        stmt.commit()
                    except:
                        pass
                        # print("no se puede comitear algo que no existe")
            except SQLAlchemyError as e:
                # print(f"Error: {e}")
                pass

        # data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        # print(f"salida: {out}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'carga_datawarehouse',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Dummy operator as a starting point
start = DummyOperator(
    task_id='start',
    dag=dag
)

list_files_task = PythonOperator(
    task_id='list_files',
    python_callable=list_files,
    dag=dag,
)

# File existence checks using FileSensor
check_google_file = FileSensor(
    task_id='check_google_file',
    filepath=f'{FILES["sites_google"]}',
    fs_conn_id='s3_bucket',
    poke_interval=10,
    timeout=600,
    dag=dag
)

check_yelp_file = FileSensor(
    task_id='check_yelp_file',
    filepath=f'{FILES["sites_yelp"]}',
    fs_conn_id='s3_bucket',
    poke_interval=10,
    timeout=600,
    dag=dag
)

# Data processing tasks
sites_google_to_dw_task = sites_google_to_dw()
sites_google_reviews_to_dw_task = sites_google_reviews_to_dw()
sites_yelp_to_dw_task = sites_yelp_to_dw()
sites_yelp_reviews_to_dw_task = sites_yelp_reviews_to_dw()

# Define task dependencies
start >> list_files_task >> [check_google_file, check_yelp_file]
check_google_file >> [sites_google_to_dw_task, sites_google_reviews_to_dw_task]
check_yelp_file >> [sites_yelp_to_dw_task, sites_yelp_reviews_to_dw_task]
