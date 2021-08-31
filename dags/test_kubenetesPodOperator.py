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

    t0 = BashOperator(
        task_id='Forecast',
        depends_on_past=False,
        bash_command="echo this is Forecast; sleep 1"
    )

    t1 = BashOperator(
        task_id='trans_1',
        depends_on_past=False,
        bash_command="echo this is trans_1; sleep 1"
    )

    t2 = KubernetesPodOperator(
        namespace='airflow',
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        cmds=["sh", "-c", "echo 'Hello, this is Fluenta from GCR...'"],
        name="ubuntu_18_0_4_1",
        task_id="Fluenta_from_GCR",
        get_logs=True
    )

    t3 = BashOperator(
        task_id='trans_2',
        depends_on_past=False,
        bash_command="echo this is trans_2; sleep 1"
    )

    t4 = KubernetesPodOperator(
        namespace='airflow',
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        cmds=["sh", "-c", "echo 'Hello, this is Dorset from GCR...'"],
        name="ubuntu_18_0_4_1",
        task_id="Dorset_from_GCR",
        get_logs=True
    )

    t5 = BashOperator(
        task_id='Planning_and_Budgeting',
        bash_command="echo Planning_and_Budgeting"
    )    
    t0 >> t1 >> t2 >> t3 >> t4 >> t5


    # start = DummyOperator(task_id='run_this_first')

    # passing = KubernetesPodOperator(namespace='default',
    #                       image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
    #                       cmds=["Python","-c"],
    #                       arguments=["print('hello world')"],
    #                       labels={"foo": "bar"},
    #                       name="ubuntu_18_0_4_1",
    #                       task_id="ubuntu_18_0_4_1",
    #                       get_logs=True
    #                       )

    # failing = KubernetesPodOperator(namespace='default',
    #                       image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
    #                       cmds=["sh", "-c", 'echo \'Sleeping..\'; echo \'Done!\''],
    #                       labels={"foo": "bar"},
    #                       name="ubuntu_18_0_4_2",
    #                       task_id="ubuntu_18_0_4_2",
    #                       get_logs=True
    #                       )
  
    # start >> [passing, failing]
