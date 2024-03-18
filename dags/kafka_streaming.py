from datetime import datetime
# from airflow import DAG 
# from airflow.operators.python import PythonOperator


def get_data():
    import json
    import requests

    res = requests.get("https://randomuser.me//api")
    if(res.status_code==200):
        format_res = (res.json()['results'][0])
        return format_res
    else:
        raise Exception(res.get('status'))



# default_args = {
#     'owner':'airflow',
#     'start_date':datetime(2024,3,17,21,00)
# }

# with DAG('user_automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False
#     ) as dag:
#     streaming_task = PythonOperator(
#         task_id = 'stream_data_from_api',
#         python_callable=stream_data
#     )


