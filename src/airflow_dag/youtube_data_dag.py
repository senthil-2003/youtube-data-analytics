from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta, timezone
from requests import post, exceptions
from traceback import format_exception

def send_discord_alert(payload_values: dict):
    conn = BaseHook.get_connection("DISCORD_ALERT_CHANNEL_WEBHOOK_URL")
    payload = {"embeds":[payload_values]}
    try:
        response = post(conn.host, json=payload)
        response.raise_for_status()
    except exceptions.RequestException as e:
        print(f"Failed to send alert to Discord: {e}")
    except exceptions.HTTPError as e:
        print(f"HTTP error occurred while sending alert to Discord: {e}")
    except Exception as e:
        print(f"An error occurred while sending alert to Discord: {e}")

def success_message(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S UTC")
    logical_date = context['logical_date'].strftime("%Y-%m-%d %H:%M:%S UTC")
    duration = datetime.now(timezone.utc) - context['dag_run'].start_date.replace(tzinfo=timezone.utc)
    
    embed ={
        "title" : f" ✅ Airflow DAG Success: {dag_id}",
        "color" : 3066993, # green color
        "fields" : [
            {"name": "DAG", "value": f"`{dag_id}`", "inline": True},
            {"name": "Status", "value": "✅ Success", "inline": True},
            {"name": "Execution Date", "value": f"`{execution_date}`", "inline" : False},
            {"name": "Logical Date", "value": f"`{logical_date}`", "inline" : False},
            {"name": "Duration", "value": f"`{str(duration).split('.')[0]}`", "inline": False},
            {"name": "Run ID", "value": f"`{run_id}`", "inline": False}
        ],
        "footer": {"text" : "Airflow DAG | YouTube Data Pipeline"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    send_discord_alert(embed)
    
def failure_message(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_id = context['task_instance'].task_id
    execution_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S UTC")
    logical_date = context['logical_date'].strftime("%Y-%m-%d %H:%M:%S UTC")
    log_url = context['task_instance'].log_url
    
    # get the exception message
    exception_message = context.get('exception', 'No exception message available')
    if exception_message:
        error_message = str(exception_message)
    else:
        error_message = "No exception message available"
    
    tb = ''.join(format_exception(type(exception_message), exception_message, exception_message.__traceback__)) if exception_message else "No traceback available"
    if len(tb) > 950:
        tb = tb[:950] + "\n\n[Truncated traceback]"
        
    try_number = context['task_instance'].try_number
    max_tries = context['task_instance'].max_tries + 1

    embed = {
        "title": f" ❌ Airflow DAG Failure: {dag_id}",
        "color": 15158332,  # red color
        "fields": [
            {"name": "DAG", "value": f"`{dag_id}`", "inline": True},
            {"name": "Status", "value": "❌ Failed", "inline": True},
            {"name": "Execution Date", "value": f"`{execution_date}`", "inline": False},
            {"name": "Logical Date", "value": f"`{logical_date}`", "inline": False},
            {"name": "Log URL", "value": f"`{log_url}`", "inline": False},
            {"name": "Run ID", "value": f"`{run_id}`", "inline": False},
            {"name": "Task ID", "value": f"`{task_id}`", "inline": False},
            {"name": "Try Number", "value": f"`{try_number}`", "inline": False},
            {"name": "Max Tries", "value": f"`{max_tries}`", "inline": False},
            {"name": "Error Message", "value": f"`{error_message}`", "inline": False},
            {"name": "Traceback", "value": f"```{tb}```", "inline": False},
        ],
        "footer": {"text": "Airflow DAG | YouTube Data Pipeline"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    send_discord_alert(embed)
    
default_args = {
    'owner' : "senthil",
    
    #retry behaviour
    'retries' : 3,
    'retry_delay' : timedelta(minutes=1),
    
    # timeouts
    'execution_timeout' : timedelta(minutes=10),
    
    'on_failure_callback': failure_message
}

@dag(dag_id="youtube_etl_dag",
     start_date=datetime(2026, 3, 22),
     schedule="30 2 * * 3",
     default_args=default_args,
     catchup=False,
     on_success_callback=success_message,
     dagrun_timeout = timedelta(minutes=60),
)
def youtube_data_pipeline_dag():

    env_path = Variable.get("youtube_data_pipeline_env_path")
    base_scripts_path = Variable.get("youtube_data_pipeline_base_scripts_path")
    
    @task.bash(env= {"RUN_DATE": "{{ ds }}"})
    def collect_raw_data():
        file_path = "src.raw_data.raw_data_handler"
        return f"cd {base_scripts_path} && {env_path} -m {file_path}"

    @task.bash(env= {"RUN_DATE": "{{ ds }}"})
    def convert_to_delta_table():
        file_path = "src.delta_lake.delta_table_pipeline"
        return f"cd {base_scripts_path} && {env_path} -m {file_path}"

    @task.bash(env= {"RUN_DATE": "{{ ds }}"})
    def convert_to_sql():
        file_path = "src.data_warehouse.data_warehouse_pipeline"
        return f"cd {base_scripts_path} && {env_path} -m {file_path}"

    @task.bash(env= {"RUN_DATE": "{{ ds }}"})
    def write_to_kaggle():
        file_path = "src.kaggle.upload_dataframe_kaggle"
        return f"cd {base_scripts_path} && {env_path} -m {file_path}"

    raw_data_task = collect_raw_data()
    delta_table_task = convert_to_delta_table()
    sql_task = convert_to_sql()
    kaggle_task = write_to_kaggle()

    raw_data_task >> delta_table_task >> sql_task >> kaggle_task

youtube_data_pipeline_dag()