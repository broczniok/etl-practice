from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from smart_file_sensor import SmartFileSensor


def start_processing(dag_id, table_name):
    print(f"{dag_id} start processing tables in database: {table_name}")


def get_time(context):
    ti = context['task_instance']
    start_time = context['execution_date']
    print("start time:", start_time)
    ti.xcom_push(key='start_time', value=start_time.isoformat())


config = {
    'sensor_dag': {'schedule_interval': "@daily", "start_date": datetime(2024, 6, 29)},
}
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

for conf_name, conf in config.items():
    with DAG(dag_id=conf_name,
             default_args=default_args,
             schedule_interval=conf['schedule_interval'],
             start_date=conf['start_date'],
             catchup=False) as dag:
        app_path = Variable.get('last_mobile_path') # idea is to set the path to file_name_number+1 and for it
        user_path = Variable.get('last_user_path') # idea is to set the path to file_name_number+1 and for it

        wait_for_app_file_task = SmartFileSensor(
            task_id='sensor_wait_app_file',
            filepath=app_path,
            fs_conn_id='fs_default',
            queue='jobs_dag'
        )

        wait_for_user_file_task = SmartFileSensor(
            task_id='sensor_wait_user_file',
            filepath=user_path,
            fs_conn_id='fs_default',
            queue='jobs_dag'
        )

        trigger_app_task = TriggerDagRunOperator(
            task_id='trigger_app_task',
            trigger_dag_id='extract_app_data_dag',
            on_success_callback=get_time,
            poke_interval=60,
            queue='jobs_dag'
        )

        trigger_user_task = TriggerDagRunOperator(
            task_id='trigger_user_task',
            trigger_dag_id='extract_user_data_dag',
            on_success_callback=get_time,
            poke_interval=60,
            queue='jobs_dag'
        )

        wait_for_user_file_task >> trigger_user_task
        wait_for_app_file_task >> trigger_app_task


