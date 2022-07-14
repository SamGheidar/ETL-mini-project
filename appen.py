from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator

import requests, os
import json
import shutil
import psycopg2 as ps
from sqlalchemy import create_engine


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
raw_path = CURR_DIR_PATH + "/raw/"
harmonized_path = CURR_DIR_PATH + "/harmonized/"
cleansed_path = CURR_DIR_PATH + "/cleansed/"

API_KEY = 'df731731dd73ec2cb7b035dd1faccf7c'
lat = 59.9
lon = 17.6
URL = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_KEY}"





def _raw_to_harmonized():
    r = requests.get(URL)
    file_and_path = os.path.join(raw_path, 'raw_data.json')
    f = open(file_and_path, 'w')
    f.write(r.text)
    f.close()


def _harmonized_to_cleansed():
    with open(raw_path +'raw_data.json') as f:
        data = json.load(f)
        harmonized_data = []
        df = pd.DataFrame(harmonized_data, columns=['date', 'temperature', 'precipitation', 'air_pressure'])
        for i in data['list']:
            weather_entry={
            'date': i['dt_txt'],
            'temperature': i['main']['temp'] -273,
            'precipitation': i['pop'],
            'air_pressure': i['main']['pressure']}
            df = df.append(weather_entry, ignore_index=True)
        df.to_json(harmonized_path + "harmonized_data.json")

    

def _cleansed_to_staged():
    shutil.copy(harmonized_path + "harmonized_data.json", cleansed_path + "cleansed_data.json")




def _staged_to_modelled():

    def postgres_creator():  # sqlalchemy requires callback-function for connections
        return ps.connect(
            dbname="etl",  # name of table
            user="etl_project",
            password='123456',
            host="localhost"
    )
    postgres_engine = create_engine(
        url="postgresql+psycopg2://localhost",  # driver identification + dbms api
        creator=postgres_creator  # connection details
    )


    a = (cleansed_path + "cleansed_data.json")

    with open(a) as json_data:
        record_list = json.load(json_data)
        df = pd.DataFrame(record_list)
        df.to_sql('staged', postgres_engine, if_exists= 'replace', index_label='index')
        #pd.read_sql('SELECT temperature, air_pressure FROM staged;', postgres_engine)
        pd.read_sql('staged', postgres_engine)




with DAG("weather_dag", start_date=datetime(2022, 1, 1),
    schedule_interval= "@daily" , catchup=False) as dag:

        raw_to_harmonized= PythonOperator(
            task_id="raw_to_harmonized",
            python_callable=_raw_to_harmonized
        )
        harmonized_to_cleansed= PythonOperator(
            task_id="harmonized_to_cleansed",
            python_callable=_harmonized_to_cleansed
        )
        cleansed_to_staged= PythonOperator(
            task_id="cleansed_to_staged",
            python_callable=_cleansed_to_staged
        )
        staged_to_modelled= PythonOperator(
            task_id="staged_to_modelled",
            python_callable=_staged_to_modelled
        )

        raw_to_harmonized >> harmonized_to_cleansed >> cleansed_to_staged >> staged_to_modelled