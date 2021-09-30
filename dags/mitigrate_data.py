import os
import sys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as dt
from datetime import timedelta
sys.path.insert(0,"../dags/scripts/")
import scripts.midata as script

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'email': ['nkarietn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': dt(2021, 9, 13),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'database_migrator_dag',
    default_args=default_args,
    description='Migrate database from MYSQL to PostgressSQL',
    schedule_interval='@daily',
)

sql = "SELECT * FROM I80_stations"
I80_stations_migrator = script.MigrationOperator(
    task_id='I80_stations_migrator',
    source_conn_id='mysql_conn_id',
    destination_conn_id='postgres_conn_id',
    destination_table="I80stations",
    sql=sql,
    dag=dag
)

sql = "SELECT * FROM richards"
richards_migrator = script.MigrationOperator(
    task_id='richards_migrator',
    source_conn_id='mysql_conn_id',
    destination_conn_id='postgres_conn_id',
    destination_table="richard12345",
    sql=sql,
    dag=dag
)

sql = "SELECT * FROM I80_davis"
I80_davis_migrator = script.MigrationOperator(
    task_id='I80_davis_migrator',
    source_conn_id='mysql_conn_id',
    destination_conn_id='postgres_conn_id',
    destination_table="I80davis",
    sql=sql,
    dag=dag
)

email = EmailOperator(task_id='send_email',
                      to='nkarietn@gmail.com@gmail.com',
                      subject='Daily report generated',
                      html_content=""" <h1>Congratulations! The tables are created.</h1> """,
                      dag=dag,
                      )

[I80_stations_migrator, richards_migrator,
    I80_davis_migrator] >> email