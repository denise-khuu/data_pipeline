from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
from airflow.settings import AIRFLOW_HOME

# Defining DAG arguments
default_args = {
    'owner': 'Denise-Phi Khuu',
    'start_date': days_ago(0),
    'email': ['denisephi.khuu@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='Access Log',
    schedule_interval=timedelta(days=1),
)

# Download and Unzip File
download = BashOperator(
    task_id='download',
    bash_command='wget -P {} "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"; gunzip -f {}/web-server-access-log.txt.gz'.format(AIRFLOW_HOME, AIRFLOW_HOME),
    dag=dag,
)

# Extract
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d# -f1,4 %s/web-server-access-log.txt > %s/extracted-data.txt; echo "$(cat %s/extracted-data.txt)"'%(AIRFLOW_HOME, AIRFLOW_HOME, AIRFLOW_HOME),
    dag=dag,
)

# Transform
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < %s/extracted-data.txt > %s/capitalized.txt'%(AIRFLOW_HOME, AIRFLOW_HOME),
    dag=dag,
)


# Load

load = BashOperator(
    task_id='load',
    bash_command='gzip %s/capitalized.txt'%(AIRFLOW_HOME),
    dag=dag,
)

download >> extract >> transform >> load

