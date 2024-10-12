import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.email import send_email

# Asegurar que se añada la ruta común si es necesario
PATH_COMMON = '../'
sys.path.append(PATH_COMMON)

# Importar las tareas correctamente desde add_task.py
from common.add_task import task_monitoringmodel, task_predictionnmodel

# Función para enviar correo con las predicciones en caso de éxito
def success_email(context):
    task_instance = context['task_instance']
    task_status = 'Success'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'

    # Obtener las predicciones del contexto
    predictions = context['task_instance'].xcom_pull(task_ids='prediction_model')

    body = f'The task {task_instance.task_id} completed successfully. \n\n'\
           f'Task execution date: {context["execution_date"]}\n\n'\
           f'Predictions: \n{predictions}\n\n'\
           f'Log url: {task_instance.log_url}\n\n'
           
    to_email = 'sojuanesi4@gmail.com'
    send_email(to=to_email, subject=subject, html_content=body)

# Función para envío de correo en caso de fallo
def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} failed with status: {task_status}. \n\n'\
           f'Task execution date: {context["execution_date"]}\n'\
           f'Log url: {task_instance.log_url}\n\n'
    to_email = 'sojuanesi4@gmail.com'  # Correo del destinatario
    send_email(to=to_email, subject=subject, html_content=body)

# Definir los argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 3, 17),
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

@dag(
    dag_id='pred_moni',
    default_args=default_args,
    on_failure_callback=failure_email,  # Enviar correo en caso de fallo
    on_success_callback=success_email,  # Enviar correo con las predicciones en caso de éxito
    schedule_interval=None,
    catchup=False
)
def mydag():
    # Definir las tareas importadas
    prediction_task = task_predictionnmodel()
    monitoring_task = task_monitoringmodel()

    # Definir las dependencias entre las tareas
    prediction_task >> monitoring_task

first_dag = mydag()
