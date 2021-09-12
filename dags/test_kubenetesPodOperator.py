from kubernetes.client import V1Volume, V1SecretVolumeSource, V1VolumeMount

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="example_k8s_secret_volume", start_date=days_ago(1), schedule_interval='@once', tags=["example"]) as dag:

    secret = Secret('volume', '/etc/my-secret', 'db-credentials')
    # Is Equal to below two lines
    # secret_volume = V1Volume(name='my-secret-vol', secret=V1SecretVolumeSource(secret_name='db-credentials'))
    # secret_volume_mount = V1VolumeMount(mount_path='/etc/my-secret', name='my-secret-vol', read_only=True)

    task1 = KubernetesPodOperator(task_id='SC_pipeline_half_real',
                                  name='airflow_pod_operator_secret_volume',
                                  namespace='default',
                                  secrets=[secret, ],
                                  # secrets is equal to below two lines
                                  # volumes=[secret_volume, ],
                                  # volume_mounts=[secret_volume_mount, ],
                                  image='alpine',
                                  cmds=["sh", "-c",
                                        'echo "Secret Directory Content "$(ls -l /etc/my-secret)'],
                                  in_cluster=False,
                                  startup_timeout_seconds=60,
                                  )
