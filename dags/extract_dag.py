import logging
from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


def get_time(_,**kwargs):
    ti = kwargs['ti']
    time = ti.xcom_pull(dag_id='sensor_dag', key='start_time', include_prior_dates=True)
    logging.info(f"Retrieved start_time from XCom: {time}")
    return datetime.fromisoformat(time) if time else None


with DAG(
        dag_id='extract_user_data_dag',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2024, 6, 29),
        catchup=False
) as dag_1:

    dag_sensor_user_task = ExternalTaskSensor(
        task_id='insert_user_rows',
        external_dag_id='sensor_dag',
        external_task_id='trigger_user_task',
        execution_date_fn=get_time,
        mode='poke',
        queue='jobs_dag',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        poke_interval=60,
        timeout=600
    )

    create_spark_container_1 = DockerOperator(
        task_id='create_spark_container_1',
        image='apache/spark:latest',
        container_name='spark-container-1',
        command='''
        /opt/spark/bin/spark-submit /spark-scripts/query_user_script.py
        ''',
        auto_remove=True,
        mounts=[
            {
                'source': '/Users/broczniok/Desktop/etl-practice/spark-scripts',
                'target': '/spark-scripts',
                'type': 'bind'
            },
            {
                'source': '/Users/broczniok/Desktop/etl-practice/parquets',
                'target': '/opt/spark/parquets',
                'type': 'bind'
            },
            {
                'source': '/Users/broczniok/Desktop/etl-practice/capstone-dataset',
                'target': '/capstone-dataset',
                'type': 'bind'
            }
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        tty=True,
        dag=dag_1,
        queue='jobs_dag'
    )

    dag_sensor_user_task >> create_spark_container_1

with DAG(
        dag_id='extract_app_data_dag',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2024, 6, 29),
        catchup=False
) as dag_2:
    dag_sensor_app_task = ExternalTaskSensor(
        task_id='insert_app_rows',
        external_dag_id='sensor_dag',
        external_task_id='trigger_app_task',
        execution_date_fn=get_time,
        mode='poke',
        queue='jobs_dag',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        poke_interval=60,
        timeout=600
    )

    create_spark_container_2 = DockerOperator(
        task_id='create_spark_container_2',
        image='apache/spark:latest',
        container_name='spark-container-2',
        command='''
        /opt/spark/bin/spark-submit /spark-scripts/query_app_script.py
        ''',
        auto_remove=True,
        mounts=[
            {
                'source': '/Users/broczniok/Desktop/etl-practice/spark-scripts',
                'target': '/spark-scripts',
                'type': 'bind'
            },
            {
                'source': '/Users/broczniok/Desktop/etl-practice/parquets',
                'target': '/opt/spark/parquets',
                'type': 'bind'
            },
            {
                'source': '/Users/broczniok/Desktop/etl-practice/capstone-dataset',
                'target': '/capstone-dataset',
                'type': 'bind'
            }
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        tty=True,
        dag=dag_2,
        queue='jobs_dag'
    )

    dag_sensor_app_task >> create_spark_container_2
