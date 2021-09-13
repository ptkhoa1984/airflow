from kubernetes.client import V1VolumeMount, V1Volume, V1PersistentVolumeClaimVolumeSource

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="example_k8s_volume", start_date=days_ago(1),
         schedule_interval='@once', tags=["example"]) as dag:
    myapp_volume = V1Volume(
        name='myapp-volume',
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name='myapp-pvc-rw'))

    myapp_volume_mount = V1VolumeMount(mount_path='/root/myapp', name='myapp-volume')
    
    task0 = KubernetesPodOperator(task_id='khoa_test',
                                 name='khoa_test',
                                 namespace='airflow',
                                 image='eu.gcr.io/skyuk-uk-dsas-poc/alpine-linux',
                                 cmds=["sh", "-c",
                                       "echo 'hello from Khoa'",
                                       ],
                                 startup_timeout_seconds=60,
                                 )

    task1 = KubernetesPodOperator(task_id='k8s_volume_read_task',
                                  name='airflow_pod_volume_read',
                                  namespace='airflow',
                                  image='eu.gcr.io/skyuk-uk-dsas-poc/alpine-linux',
                                  volumes=[myapp_volume, ],
                                  volume_mounts=[myapp_volume_mount, ],
                                  cmds=["sh", "-c",
                                        'date > /root/myapp/date.txt',
                                        ],
                                  startup_timeout_seconds=60,
                                  )

    task2 = KubernetesPodOperator(task_id='k8s_volume_write_task',
                                  name='airflow_pod_volume_write',
                                  namespace='airflow',
                                  image='eu.gcr.io/skyuk-uk-dsas-poc/alpine-linux',
                                  volumes=[myapp_volume, ],
                                  volume_mounts=[myapp_volume_mount, ],
                                  cmds=["sh", "-c",
                                        'echo "Reading date from date.txt : "$(cat /root/myapp/date.txt)',
                                        ],
                                  startup_timeout_seconds=60,
                                  )

    task0 >> task1 >> task2
