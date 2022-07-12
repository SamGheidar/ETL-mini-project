from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests, os
import matplotlib.pyplot as plt


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
raw_dir = CURR_DIR_PATH + "/raw/"
API_KEY = 'df731731dd73ec2cb7b035dd1faccf7c'
lat = 59.9
lon = 17.6
URL = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_KEY}"
r = requests.get(URL)




def _raw_to_harmonized():
    df = pd.DataFrame(r)
    return df.to_json(raw_dir + "raw_data.json")

def _harmonized_to_cleansed():
    pass

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