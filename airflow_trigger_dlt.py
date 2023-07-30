from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'airflow'
}

with DAG('dlt',
         start_date=days_ago(2),
         schedule_interval="@once",
         default_args=default_args
         ) as dag:
    opr_run_now=DatabricksSubmitRunOperator(
        task_id='run_now',
        databricks_conn_id='CONNECTION_ID',
        pipeline_task={"pipeline_id":"48390438-20482-32-9243-234"}
    )