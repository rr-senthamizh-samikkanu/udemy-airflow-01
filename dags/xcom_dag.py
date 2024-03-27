from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
def _t1(ti):
    # return 40  # one way to push to xcom with default 'return_value' key
    ti.xcom_push(key='senth_value', value=41)  # custom_key
 
def _t2(ti):
    print('I am able to READ from another task here... viola', ti.xcom_pull(key='senth_value', task_ids='t1'))

def _branch(ti):
    value = ti.xcom_pull(key='senth_value', task_ids='t1')
    if value == 45:
        return 't2'
    return 't3'
 
with DAG("xcom_dag", start_date=datetime(2023, 3, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )
 

    t4 = BashOperator(
        task_id='t4',
        bash_command="echo 'SENTHHHHH'",
        trigger_rule='none_failed_min_one_success'  # 'all_success'
    )

    t1 >> branch >> [t2, t3] >> t4