#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
# [START import_module]
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from textwrap import dedent
from kubernetes.client import models as k8s

# [END import_module]

############################### common variables ###############################
FORECAST_ID = 2417
CDN_PLAN_ID = 2359
TERRITORY = "UK"
FAILOVER_ID = 33
ID = 343
T0_COMMAND = f"select * from inventory.end2end_path where id = {ID}"
PERIOD = "'Y20.06'"

####################### variables when running on premise ######################
BASE_PATH = "/Users/tpp02/OneDrive\ -\ Sky/code"

T1_COMMAND = f"/opt/data/core_model/core_model/planning/Fluenta/fluenta_plan_w_sql.py \
        --forecast_id={FORECAST_ID} \
        --cdn_plan_id={CDN_PLAN_ID} \
        --territory={TERRITORY} \
        --failover_id={FAILOVER_ID} \
        --filename /opt/data/core_model/core_model/planning/Fluenta/khoa --soip"

T2_COMMAND = f"python /opt/data/fluenta-master/fluenta/app.py \
        -path_in /opt/data/fluenta-master/tests/test_data/test_data_plan_p350.txt \
        -filename /opt/data/fluenta-master/fluenta/out \
        -period {PERIOD}"

T3_COMMAND = f"python {BASE_PATH}/khoa_code/airflow_pip/transform_fluenta_output.py"

T4_COMMAND = f"python /opt/data/dorset-develop/poc/pop_tactical/main_pop_tactical.py"

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',    
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'SC_pipeline_half_real',
    default_args=default_args,
    description='SC_pipeline_half_real',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['SC_pipeline_half_real'],
    catchup=False
) as dag:
    # [END instantiate_dag]


    # add username/pass/url in postgres_default (in Admin/Connections/postgres_default)
    t0 = SqlSensor(
        task_id='Forecast',
        conn_id='postgres_default',
        sql=T0_COMMAND
    )

    t1 = KubernetesPodOperator(
        namespace='airflow',
        image='eu.gcr.io/skyuk-uk-dsas-poc/kp-core-model-ubuntu:0.1',
        cmds=["sh", "-c", T1_COMMAND],
        name="trans_1",
        task_id="trans_1",
        get_logs=True
    )


    # t1 = BashOperator(
    #     task_id='trans_1',
    #     depends_on_past=False,
    #     bash_command=T1_COMMAND
    # )

    # t2 = BashOperator(
    #     task_id='Fluenta',
    #     depends_on_past=False,
    #     bash_command=T2_COMMAND
    # )

    t2 = KubernetesPodOperator(
        namespace='airflow',
        image='eu.gcr.io/skyuk-uk-dsas-poc/kp-fluenta-ubuntu:0.1',
        cmds=["sh", "-c", T2_COMMAND],
        name="Fluenta",
        task_id="Fluenta",
        get_logs=True
    )


    t3 = BashOperator(
        task_id='trans_2',
        depends_on_past=False,
        # bash_command=T3_COMMAND,
        bash_command="echo 'This is trans_2'"
    )

    # t4 = BashOperator(
    #     task_id='Dorset',
    #     bash_command=T4_COMMAND
    # )

    t4 = KubernetesPodOperator(
        namespace='airflow',
        image='eu.gcr.io/skyuk-uk-dsas-poc/kp-dorset-ubuntu:0.1',
        cmds=["sh", "-c", T4_COMMAND],
        name="Dorset",
        task_id="Dorset",
        get_logs=True
    ) 

    t5 = BashOperator(
        task_id='Planning_and_Budgeting',
        bash_command="echo 'This is Planning_and_Budgeting'"
    )    
    t0 >> t1 >> t2 >> t3 >> t4 >> t5


# [END tutorial]
