from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests, os
import json



CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
raw_path = CURR_DIR_PATH + "/raw/"
harmonized_path = CURR_DIR_PATH + "/harmonized/"
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
    pass

def _staged_to_modelled():
    pass



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