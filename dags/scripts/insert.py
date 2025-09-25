import os
import glob
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "purwadika") #change into your gcp project_id

def get_bq_client():
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS","/opt/airflow/credentials/credentials.json")
    return bigquery.Client.from_service_account_json(creds, project=PROJECT_ID)

def get_latest_csv(prefix, csv_dir="/opt/airflow/generated_dummy_data"):
    pattern = os.path.join(csv_dir, f"{prefix}_*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No CSV found with prefix {prefix} in {csv_dir}")
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

def insert_csv_to_bq(local_csv_path, dataset, table, write_disposition="WRITE_APPEND"):
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=write_disposition
    )
    with open(local_csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
    print(f"insert {local_csv_path} into {table_id} ({write_disposition})")
