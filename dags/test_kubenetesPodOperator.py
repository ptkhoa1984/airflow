from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

from textwrap import dedent

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'kubernetes_sample',
    default_args=default_args,
    description='kubernetes_sample',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['kubernetes_sample'],
    catchup=False # the scheduler creates a DAG run only for the latest interval
) as dag:
    # [END instantiate_dag]

    start = DummyOperator(task_id='run_this_first1')
    # start2 = DummyOperator(task_id='run_this_first2')

    passing = KubernetesPodOperator(namespace='default',
                          image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
                          cmds=["Python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="ubuntu_18_0_4_1",
                          task_id="ubuntu_18_0_4_1",
                          get_logs=True
                          )

    failing = KubernetesPodOperator(namespace='default',
                          image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
                          cmds=["sh", "-c", 'echo \'Sleeping..\'; echo \'Done!\''],
                          labels={"foo": "bar"},
                          name="ubuntu_18_0_4_2",
                          task_id="ubuntu_18_0_4_2",
                          get_logs=True
                          )
  
    start >> [passing, failing]
    # start1 >> start2