import os
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "purwadika") #change into your gcp project_id

def get_bq_client():
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/credentials/credentials.json")
    return bigquery.Client.from_service_account_json(credentials_path, project=PROJECT_ID)

def run_query(query, destination=None, overwrite=False):
    client = get_bq_client()
    job_config = QueryJobConfig()
    if destination:
        job_config.destination = f"{PROJECT_ID}.{destination}"
        job_config.write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    print("Query success.")

def run_model_mart_queries():
    #fact_transaction
    run_query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.jcdeol005_final_project_zhafar_model.fact_transaction` AS
        select 
        s.transaction_id, 
        concat(s.product,"_", s.color) as product_id, 
        sum(quantity) as quantity, 
        CASE WHEN avg(d.discount) is null then sum(revenue)
        ELSE sum(revenue)*(1-(avg(d.discount)/100)) END as revenue_after_discount,
        CASE WHEN avg(d.discount) is null then 0
        ELSE sum(revenue)-sum(revenue)*(1-(avg(d.discount)/100)) END as discount_value
        from `{PROJECT_ID}.jcdeol005_final_project_zhafar_staging.staging_transaction` s
        left join `{PROJECT_ID}.jcdeol005_final_project_zhafar_staging.staging_transaction_discount` d
        on s.transaction_id = d.transaction_id
        group by 1,2
    """, overwrite=False)

    #dim_info
    run_query(f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.jcdeol005_final_project_zhafar_model.dim_info` AS
        select 
        distinct s.transaction_id, s.email, s.channel, s.paid_at, s.status, s.sent_to, 
        case when d.campaign_name is null then 'No Campaign' else d.campaign_name end as campaign_name
        from `{PROJECT_ID}.jcdeol005_final_project_zhafar_staging.staging_transaction` s
        left join `{PROJECT_ID}.jcdeol005_final_project_zhafar_staging.staging_transaction_discount` d
        on s.transaction_id = d.transaction_id
    """, overwrite=False)

    #dim_product
    run_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.jcdeol005_final_project_zhafar_model.dim_product` AS
    select distinct concat(s.product,"_", s.color) as product_id, product, color
    from `{PROJECT_ID}.jcdeol005_final_project_zhafar_staging.staging_transaction` s
    """, overwrite=False)

    #aggregated per transaction
    run_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.jcdeol005_final_project_zhafar_marts.agg_info_per_transaction` AS
    select 
    f.transaction_id, date_trunc(i.paid_at,day) as paid_at, i.campaign_name, i.email, i.status, i.sent_to, p.product, p.color,
    f.quantity, f.revenue_after_discount, f.discount_value
    from `{PROJECT_ID}.jcdeol005_final_project_zhafar_model.fact_transaction` f
    left join `{PROJECT_ID}.jcdeol005_final_project_zhafar_model.dim_info` i on f.transaction_id = i.transaction_id
    left join `{PROJECT_ID}.jcdeol005_final_project_zhafar_model.dim_product` p on f.product_id = p.product_id
    """, overwrite=False)

    #RFM clustering
    run_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.jcdeol005_final_project_zhafar_marts.agg_rfm_cluster` AS
    with raw_data as 
    (
    select 
    i.email, 
    NTILE(5) OVER (ORDER BY max(i.paid_at) asc) AS recency_rank,
    NTILE(5) OVER (ORDER BY count(distinct f.transaction_id) asc) AS frequency_rank,
    NTILE(5) OVER (ORDER BY sum(f.revenue_after_discount) asc) AS monetary_rank
    from {PROJECT_ID}.jcdeol005_final_project_zhafar_model.fact_transaction f
    left join {PROJECT_ID}.jcdeol005_final_project_zhafar_model.dim_info i on f.transaction_id = i.transaction_id
    where i.status != 'cancel' and i.status != 'returned'
    group by 1
    )
    select
    email, 
    case
    when recency_rank between 4 and 5 and frequency_rank between 4 and 5 and monetary_rank between 4 and 5 then 'Top Spender'
    when recency_rank between 3 and 5 and frequency_rank between 4 and 5 and monetary_rank between 3 and 5 then 'Loyal Customer'
    when recency_rank between 2 and 5 and frequency_rank between 2 and 5 and monetary_rank between 4 and 5 then 'Big Spender'
    when recency_rank between 1 and 2 and frequency_rank between 3 and 5 and monetary_rank between 3 and 5 then 'At Risk'
    when recency_rank between 1 and 2 and frequency_rank between 1 and 2 and monetary_rank between 1 and 2 then 'Hibernating'
    when recency_rank between 4 and 5 and frequency_rank between 1 and 2 and monetary_rank between 1 and 2 then 'New Customer'
    when recency_rank between 2 and 3 and frequency_rank between 2 and 3 and monetary_rank between 2 and 3 then 'Need Attention'
    when recency_rank = 1 then 'Lost Customer'
    else 'General Customer'
    end as rfm_cluster
    from raw_data
    """, overwrite=False)

    print("All model & mart queries finished.")
