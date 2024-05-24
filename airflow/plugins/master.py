import pandas as pd
import numpy as np
import json
import datetime
import os
import pyarrow.parquet as pq

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError

import warnings
warnings.filterwarnings('ignore')


class ETL:

    def __init__(self):
        self.state_abreviations = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", 
            "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", 
            "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", 
            "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", 
            "WI", "WY"
        ]

        self.state_dictionary = {
            "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", 
            "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", 
            "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", 
            "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", 
            "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", 
            "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", 
            "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", 
            "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", 
            "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", 
            "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", 
            "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", 
            "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
        }

        self.top_5 = []

        
    def extract_google_sites(self):
        def get_state_ab(st):
            try:
                state = st.split(', ')[-1].split(' ')[0]
                if state in self.state_abreviations:
                    return state
                else:
                    return np.nan
            except:
                return np.nan        
        # Tiempo de Demora Medio: 51 segundos.
        lineas_json = []

        stop = 1000
        count = 0

        # Son 11 archivos con un ordinal, del 1 al 11
        for i in range(1, 12):
            path = f'datasets/Google Maps/metadata-sitios/{i}.json'
            with open(path, 'r') as file:
                for l in file:
                    try:
                        linea_j = json.loads(l)
                        if 'restaurant' in " ".join(linea_j['category']).lower():
                            lineas_json.append(linea_j)
                            count += 1
                            if count >= stop:
                                print("limite alcanzado")
                                break
                    except:
                        pass

        df = pd.DataFrame(lineas_json)
            
        df['state_ab'] = df['address'].apply(get_state_ab)
        
        self.top_5 = df['state_ab'].value_counts().head(5).index.to_list()

        with open('top_5.json', 'w') as f:
            json.dump(self.top_5, f)


        df = df[df['state_ab'].isin(self.top_5)]
        df.head(1000).to_html("data/generated/Google/sitios.html")
        df.to_parquet('data/generated/Google/metada_sitios.parquet')


    def extract_google_review_sites(self):
        # Deserialize top_5 from the file
        with open('top_5.json', 'r') as f:
            self.top_5 = json.load(f)

        with open("top_5.txt", 'w') as f:
            f.write(f'top 5 {self.top_5}')

        self.top_5_url = [f"datasets/Google Maps/reviews-estados/review-{self.state_dictionary[i].replace(' ', '_')}/" for i in self.top_5]

        cantidad_archivos = {}

        for i in self.top_5_url:
            for j in os.walk(i):
                cantidad_archivos[i] = len(j[2])

        lineas_json_revs_google = []

        for i in self.top_5_url:
            count = 0
            for c in range(1,cantidad_archivos[i]+1):
                with open(str(i)+str(c)+".json", 'r', encoding='utf-8') as f:        
                    for s in f:
                        linea = json.loads(s)
                        linea['anio'] = datetime.datetime.fromtimestamp(linea['time']/1000).year
                        linea['estado'] = i.split('-')[-1][:-1]
                        
                        if linea['anio'] in [2017,2018,2019]:
                            lineas_json_revs_google.append(linea)
                        count += 1
                        if count > 1000:
                            break

            df_revs_google = pd.DataFrame(lineas_json_revs_google)

            df_revs_google.to_parquet('data/generated/Google/reviews-estados.parquet')

        
    def extract_yelp_sites(self):
        with open('top_5.json', 'r') as f:
            self.top_5 = json.load(f)

        with open("top_5.txt", 'w') as f:
            f.write(f'top 5 {self.top_5}')

        url_business = r'datasets/Yelp/business.pkl'
        df_business = pd.read_pickle(url_business)
        df_business = df_business.iloc[:,:-14]
        df_business = df_business[df_business.state.isin(self.top_5)]

        def is_restaurant(st):
            try: 
                test = "".join(st).lower()
                return 'restaurant' in test
            except:
                return False

        df_business = df_business[df_business['categories'].apply(is_restaurant)]
        df_business.to_parquet('data/generated/Yelp/business.parquet')

    def extract_yelp_checkin(self):
        with open('top_5.json', 'r') as f:
            self.top_5 = json.load(f)

        lineas_json = []
        path_checkin = r'datasets/Yelp/checkin.json'
        with open(path_checkin, 'r', encoding='utf-8') as file:
            for l in file:
                try:
                    linea_j = json.loads(l)
                    anio = linea_j['date'][:4]
                    if anio in ['2017', '2018', '2019']:
                        lineas_json.append(linea_j)
                except:
                    pass

        df_checkin = pd.DataFrame(lineas_json)
        df_checkin.to_parquet(r'data/generated/Yelp/checkin.parquet')

    def extract_yelp_tips(self):
        with open('top_5.json', 'r') as f:
            self.top_5 = json.load(f)

        lineas_json = []
        path_tip = r'datasets/Yelp/tip.json'
        with open(path_tip, 'r', encoding='utf-8') as file:
            for l in file:
                try:
                    linea_j = json.loads(l)
                    anio = linea_j['date'][:4]
                    if anio in ['2017', '2018', '2019']:
                        lineas_json.append(linea_j)
                except:
                    pass

        df_tip = pd.DataFrame(lineas_json)
        df_tip.to_parquet(r'data/generated/Yelp/tip.parquet')


    def extract_yelp_reviews(self):
        with open('top_5.json', 'r') as f:
            self.top_5 = json.load(f)

        df_reviews_url =  'datasets/Yelp/review.json'
        lineas_json_review = []

        with open(df_reviews_url, 'r', encoding='utf-8') as f:
            count = 0
            for i in f:
                linea = json.loads(i)
                anio = linea['date'][:4]
                if anio in ['2017', '2018', '2019'] and linea['useful'] == 1:    
                    lineas_json_review.append(linea)


        df_reviews = pd.DataFrame(lineas_json_review)
        df_reviews['funny'] = df_reviews['funny'].astype('int8')
        df_reviews['stars'] = df_reviews['stars'].astype('int8')
        df_reviews['cool'] = df_reviews['cool'].astype('int8')
        df_reviews.drop('useful', axis=1, inplace=True, errors='ignore')

        df_reviews.to_parquet('data/generated/Yelp/review.parquet')

    def extract_yelp_users(self):
        parquet_file = pq.ParquetFile(r'datasets/Yelp/user.parquet')

        arr_df = []
        count = 0
        for batch in parquet_file.iter_batches():
            # print(f"batch nro: {batch}")
            count = count +1
            batch_df = batch.to_pandas()
            batch_df['elite'] = batch_df['elite'].apply(lambda x: x.split(','))
            batch_df['elite_len'] = batch_df['elite'].apply(lambda x: len(x))
            batch_df = batch_df.query("elite_len > 1")
            arr_df.append(batch_df)
            if count >= 1:
                break

        df_users = pd.concat(arr_df)
        df_users.reset_index(inplace=True)
        df_users.drop('index', axis=1, inplace=True, errors="ignore")
        df_users.to_parquet("data/generated/Yelp/users_extracted.parquet")


# etl = ETL()

# etl.extract_yelp_sites()

class CargaInicial:

    def __init__(self):
        self.state_abreviations = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", 
            "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", 
            "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", 
            "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", 
            "WI", "WY"
        ]

        self.state_dictionary = {
            "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", 
            "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", 
            "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", 
            "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", 
            "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", 
            "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", 
            "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", 
            "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", 
            "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", 
            "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", 
            "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", 
            "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming"
        }

        self.top_5 = []

        
    def extract_sites(self):
        df_restaurantes = pd.read_parquet('data/generated/restaurantes.parquet')
        top_20 = df_restaurantes['nombre'].value_counts().head(20).index
        df_restaurantes_filtrado = df_restaurantes[df_restaurantes['nombre'].isin(top_20[1:])]
        df_restaurantes_filtrado['atributos'] = df_restaurantes_filtrado['atributos'].astype(str)

        DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-master:5432/warehouse'

        engine = create_engine(DATABASE_URI)

        stmt = engine.connect()
        metadata = MetaData()
        table = Table('restaurantes', metadata, autoload_with=engine)

        print("Comenzando inserción de datos en DW...")
        errors = 0

        for i, v in df_restaurantes_filtrado.iterrows():    
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
        
        print(f"La cantidad de errores es {errors}")

    def extract_reviews(self):
        df_reviews = pd.read_parquet('data/generated/reviews.parquet')
        df_reviews_filtradas = df_reviews[df_reviews['anio'].isin([2017, 2018, 2019])]

        DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-master:5432/warehouse'
        engine = create_engine(DATABASE_URI)
        stmt = engine.connect()
        metadata = MetaData()

        table = Table('reviews', metadata, autoload_with=engine)

        print("Comenzando inserción de datos en DW...")
        errors = 0

        for i, v in df_reviews_filtradas.iterrows():    
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
        
        print(f"La cantidad de errores es {errors}")


    def extract_states(self):
        df_estados = pd.read_parquet('data/generated/estados.parquet')

        DATABASE_URI = 'postgresql+psycopg2://airflow_user:airflow_pass@ptf-master:5432/warehouse'
        engine = create_engine(DATABASE_URI)
        stmt = engine.connect()
        metadata = MetaData()

        table = Table('estados', metadata, autoload_with=engine)

        print("Comenzando inserción de datos en DW...")
        errors = 0

        for i, v in df_estados.iterrows():    
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
        
        print(f"La cantidad de errores es {errors}")


cargaInicial = CargaInicial()

cargaInicial.extract_sites()