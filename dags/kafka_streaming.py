from datetime import datetime
from airflow import DAG 
from airflow.operators.python import PythonOperator


def get_data():
    import json
    import requests

    res = requests.get("https://randomuser.me//api")
    if(res.status_code==200):
        format_res = (res.json()['results'][0])
        return format_res
    else:
        raise Exception(res.get('status'))

def format_data(res):
    data = {}

    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

default_args = {
    'owner':'airflow',
    'start_date':datetime(2024,3,17,21,00)
}

with DAG('user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
    ) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable=stream_data
    )


