from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from suds.client import Client

from airflow.operators.python_operator import PythonOperator
import json
from datetime import datetime, timedelta

# Définir la tâche maîtresse qui lit le fichier JSON et parcourt les éléments
def read_json_file(file_path, **kwargs):
    # Code pour lire le fichier JSON et parcourir les éléments
    # ...
    print("JSON FILE TASK")
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        return data

# Définir les quatre tâches qui effectuent les appels aux services Web
def call_webservice_1(**kwargs):
    # Code pour effectuer l'appel au service Web 1
    # ...
    print("WEBSERVICE 1")

def call_webservice_2(**kwargs):
    # Code pour effectuer l'appel au service Web 2
    # ...
    print("WEBSERVICE 2")

def call_webservice_3(wsdl_url, parameters, **kwargs):
    # Code pour effectuer l'appel au service Web 3
    # ...
    client = Client(wsdl_url)

    # Appeler la méthode du service web en passant les paramètres
    result = client.ServiceIdAuteur.auteur(parameters)

    # Afficher le résultat
    print(result)

    #print("WEBSERVICE 3")

# Définir le DAG
dag = DAG(
    dag_id='DAG_TAHER',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1)
)

call_webservice_3_task = PythonOperator(
    task_id='call_webservice_3_task',
    python_callable=call_webservice_3,
    op_args=["http://10.188.228.128:8012/ServiceIdAuteur/?wsdl", 'versailles.json'],
    dag=dag
)

call_webservice_2_task = PythonOperator(
    task_id='call_webservice_2_task',
    python_callable=call_webservice_2,
    #op_args=[wsdl_url, parameters],
    dag=dag
)

call_webservice_1_task = PythonOperator(
    task_id='call_webservice_1_task',
    python_callable=call_webservice_1,
    #op_args=[wsdl_url, parameters],
    dag=dag
)

read_json_task = PythonOperator(
    task_id='read_json_task',
    python_callable=read_json_file,
    op_args=["root/airflow/test/v.json"],
    dag=dag
)

read_json_task >> call_webservice_1_task >> call_webservice_2_task >> call_webservice_3_task
