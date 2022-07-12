from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests, os
import json
import pprint


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
raw_path = CURR_DIR_PATH + "/raw/"
harmonized_path = CURR_DIR_PATH + "/harmonized/"
API_KEY = 'df731731dd73ec2cb7b035dd1faccf7c'
lat = 59.9
lon = 17.6
URL = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API_KEY}"
r = requests.get(URL)




def _raw_to_harmonized():
    df = pd.DataFrame(r)
    return df.to_json(raw_path + "raw_data.json")

def _harmonized_to_cleansed():
    with open(raw_path + 'raw_data.json') as f:
        data = json.load(f)
        #pprint.pprint(data)
    harmonized_data = []
    for entry in zip(data):
        entry = entry.to_dict("records")[0]
        weather_entry = {
            "date": entry["list.dt"],
            "temperature": entry["main.temp"] - 273,
            "air_pressure": entry["main.pressure"],
            "precipitation": entry["list.pop"]
        }
        harmonized_data.append(weather_entry)

    df = pd.DataFrame(harmonized_data, columns=["date", "tempature", "air_pressure", "precipitation"])
    df.to_json(harmonized_path + "harmonized.jason")
        

    

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