from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago

import pandas as pd
import requests

# connection server sql
MYSQL_CONNECTION = "mysql_default" 


# path data ใน airflow
mysql_output_path = "/home/airflow/data/transaction.csv"    



def get_data_from_mysql(transaction_path):

    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    df = audible_transaction.merge(audible_data,
                                        how='left',
                                        left_on='book_id',
                                        right_on='Book_ID')

    # save ไฟล์ csv transaction_path
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


# Instantiate a Dag
with DAG(
    dag_id = "Book_dag_V2",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:

    # Task
    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )
 
    # Setting up Dependencies
    t1
