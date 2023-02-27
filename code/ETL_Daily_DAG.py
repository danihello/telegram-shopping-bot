from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
	'depends_on_past': False
}

dag = DAG(
    dag_id='Daily_ETL',
    default_args=default_args,
    schedule_interval= '00 09 * * *',
	is_paused_upon_creation=False,
	catchup=False)

producer_products_task = BashOperator(
task_id='producer_products',
bash_command='python /home/naya/finalproject/code/pycharm_producer_products_vs.py',
dag=dag)

producer_stores_task = BashOperator(
task_id='producer_stores',
bash_command='python /home/naya/finalproject/code/pycharm_producer_stores_vs.py',
dag=dag)

files_parsing_task = BashOperator(
task_id='files_parsing',
bash_command='python /home/naya/finalproject/code/s3_viz.py',
dag=dag)

producer_products_task >> producer_stores_task >> files_parsing_task

if __name__ == "__main__":
    dag.cli()
	