import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
default_dag_args = {
    'start_date': datetime.datetime(2022, 6, 28),
}

PROJECT_ID = 'hardy-position-352014'
REGION = 'us-west1'
CLUSTER_NAME = 'test-dataproc-cluster'
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": f"gs://orders-customers-bucket/orders-payment/pyspark-overwrite_table.py", \
    "jar_files":f"gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"},
}



with models.DAG(
        'composer_pyspark_dag_v1',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
        
        # This task will create the dataprc cluster using the DataprocCreateClusterOperator

        create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
)
        # This task will submit the pyspark job using the DataprocSubmitJobOperator

        submit_pyspark = DataProcPySparkOperator(
            task_id='run_dataproc_pyspark',
            main='gs://orders-customers-bucket/orders-payment/pyspark-overwrite_table.py',
            cluster_name=CLUSTER_NAME,
            dataproc_pyspark_jars='gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',
            region=REGION
        )
       
        # This task will delete the cluster 

        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster",
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_NAME,
            region=REGION,
)
        # creating the dependencies among the tasks
        # below dependency first create the cluster, then submit the pyspark job and then delete the cluster

        create_cluster >> submit_pyspark >> delete_cluster