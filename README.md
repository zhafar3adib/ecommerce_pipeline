# ecommerce_pipeline
This repo will demonstrate a simple end to end ecommerce pipeline using dummy data, this pipeline overview will looks like this:

![pipeline_overview](https://github.com/zhafar3adib/ecommerce_pipeline/blob/main/images/pipeline_overview.png) 

tools needed:
- **python** to generate dummy_data
- **apache airflow** for data orchestration
- **looker** for data visualization
- **google bigquery** for data mart and data lake
- **docker** to create image container

This pipeline will divided into 3 dag task, you can see the detail in [dags](https://github.com/zhafar3adib/ecommerce_pipeline/tree/main/dags) folder 
## 1. generate task
Generate dummy ecommerce transaction data and also create list of campaign detail, the transaction that happen between campaign periode will automatically listed as transaction that will get discount.
Generated data will saved into 'generated_dummy_data' folder as a csv.

## 2. insert task
Insert the csv into raw and staging schema in google bigquery via service account. Insert to raw schema will overwrite the table, but insert to staging schema will append the table

## 3. run queries task
Run query to create table in model and mart schema. In model schema, fact and dimension table will created from staging schema, the relation looks like this:

![model_schema](https://github.com/zhafar3adib/ecommerce_pipeline/blob/main/images/model_schema.png)

In mart schema, aggregated table will created from model schema, this table will become the source of [looker](https://lookerstudio.google.com/reporting/e2885c8e-353a-4868-bcbe-f6cc6aefd257) dashboard.
Looker dashboard have 2 pages:

sales insight:

![sales_insight](https://github.com/zhafar3adib/ecommerce_pipeline/blob/main/images/looker_dashboard_1.png)

campaign insight:

![campaign_insight](https://github.com/zhafar3adib/ecommerce_pipeline/blob/main/images/looker_dashboard_2.png)

I hope this repository will help you or just find a reference to create a similar program.
