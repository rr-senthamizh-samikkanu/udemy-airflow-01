from airflow import DAG
from airflow.operators.bash import BashOperator

from groups.downloads import downloads
from groups.transforms import transforms

from datetime import datetime
 
with DAG('group_dag', start_date=datetime(2023, 3, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    args = {
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup
    }

    downloads = downloads()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms = transforms()

    downloads >> check_files >> transforms