from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner' : "senthil",
    
    #retry behaviour
    'retries' : 3,
    'retry_delay' : timedelta(minutes=1),
    
    # timeouts
    'execution_timeout' : timedelta(minutes=10)
}

@dag(dag_id="youtube_etl_dag", 
     start_date= datetime(2026, 3, 22), 
     schedule = "@daily", 
     default_args = default_args, 
     catchup=False,
    dagrun_timeout = timedelta(minutes=60),
)
def youtube_data_pipeline_dag():

    env_path = Variable.get("youtube_data_pipeline_env_path")
    base_scripts_path = Variable.get("youtube_data_pipeline_base_scripts_path")
    email_id = Variable.get("youtube_data_pipeline_email_notification").split(",")
    
    email_args = {
        "email" : email_id,
        "email_on_failure" : True,
        "email_on_retry" : False
    }
    
    @task.bash(env= {"RUN_DATE": "{{ ds }}"}, **email_args)
    def collect_raw_data():
        file_path = "raw_data/raw_data_handler.py"
        return f"{env_path} {base_scripts_path}/{file_path}"

    @task.bash(env= {"RUN_DATE": "{{ ds }}"}, **email_args)
    def convert_to_delta_table():
        file_path = "delta_lake/delta_table_pipeline.py"
        return f"{env_path} {base_scripts_path}/{file_path}"

    @task.bash(env= {"RUN_DATE": "{{ ds }}"}, **email_args)
    def convert_to_sql():
        file_path = "data_warehouse/data_warehouse_pipeline.py"
        return f"{env_path} {base_scripts_path}/{file_path}"

    @task.bash(env= {"RUN_DATE": "{{ ds }}"}, **email_args)
    def write_to_kaggle():
        file_path = "kaggle/kaggle_pipeline.py"
        return f"{env_path} {base_scripts_path}/{file_path}"

    raw_data_task = collect_raw_data()
    delta_table_task = convert_to_delta_table()
    sql_task = convert_to_sql()
    kaggle_task = write_to_kaggle()

    raw_data_task >> delta_table_task >> sql_task >> kaggle_task

youtube_data_pipeline_dag()