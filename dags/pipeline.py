from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.generator import generate_random_data, enrich_with_discount, save_csv
from scripts.insert import insert_csv_to_bq, get_latest_csv
from scripts.run_query import run_model_mart_queries

default_args = {
    "owner": "airflow",
    "depends_on_past": False
}

with DAG(
    dag_id="daily_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["etl"]
) as dag:

    def generate_task(**kwargs):
        df_trx = generate_random_data(10000,20,365,1000)
        trx_path = save_csv(df_trx, prefix="transactions")

        df_diskon = enrich_with_discount(df_trx)
        diskon_path = save_csv(df_diskon, prefix="discounts")

        return {"trx": trx_path, "diskon": diskon_path}

    def insert_task(**kwargs):
        ti = kwargs["ti"]
        paths = ti.xcom_pull(task_ids="generate_dummy_data")
        trx_path = paths["trx"]
        diskon_path = paths["diskon"]

        # transaction
        insert_csv_to_bq(trx_path, "jcdeol005_final_project_zhafar_raw", "raw_transaction", "WRITE_TRUNCATE")
        insert_csv_to_bq(trx_path, "jcdeol005_final_project_zhafar_staging", "staging_transaction", "WRITE_APPEND")

        # discount
        insert_csv_to_bq(diskon_path, "jcdeol005_final_project_zhafar_raw", "raw_transaction_discount", "WRITE_TRUNCATE")
        insert_csv_to_bq(diskon_path, "jcdeol005_final_project_zhafar_staging", "staging_transaction_discount", "WRITE_APPEND")

    def run_queries_task(**kwargs):
        run_model_mart_queries()

    t1 = PythonOperator(task_id="generate_dummy_data", python_callable=generate_task)
    t2 = PythonOperator(task_id="insert_to_bq", python_callable=insert_task)
    t3 = PythonOperator(task_id="run_queries", python_callable=run_queries_task)

    t1 >> t2 >> t3
