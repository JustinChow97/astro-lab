"""
## Real Use Case DAG

This dag pulls data from the FakeStoreAPI, lands the data into S3, copies into Snowflake, then transforms for consumption

The dag is as follows
1. Fetch_API_Data
2. Upload to S3
3. Copy into Snowflake
4. Transform

"""

from xxlimited import new
import requests
from airflow.sdk import dag, task, task_group
from pendulum import datetime, duration
from airflow.providers.standard.operators.empty import EmptyOperator

from airflow.sdk import chain, chain_linear
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



@dag(
    start_date=datetime(2025, 1, 1),
    schedule="0 9 * * *",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": duration(minutes=5),
    },
    tags=["elt", "demo"],
)
def finance_elt_dag():

    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")


    @task_group(group_id="api_extracts", tooltip="Parallel API Extractions")
    def extract_taskgroup():
        # NetSuite GL accounts extraction
        @task(task_id="extract_netsuite", queue="api-extracts")
        def extract_netsuite():
            return "Successfully extracted data from NetSuite"
        
        # Salesforce revenue pipeline  
        @task(task_id="extract_salesforce", queue="api-extracts")
        def extract_salesforce():
            return "Successfully extracted data from Salesforce"
        
        # Workday headcount
        @task(task_id="extract_workday", queue="api-extracts")
        def extract_workday():
            return "Successfully extracted data from Workday"
        
        # Asana project costs
        @task(task_id="extract_asana", queue="api-extracts")
        def extract_asana():
            return "Successfully extracted data from Asana"
        
        # All 4 run in parallel automatically within TaskGroup
        extract_netsuite(), extract_salesforce(), extract_workday(), extract_asana()
    

    # Loads raw JSON list into S3 Bucket
    load_to_S3 = EmptyOperator(task_id="load_to_s3")
        
    # Perform Copy into from S3 External Stage to Raw Schema
    copy_to_bronze =  EmptyOperator(task_id="copy_to_bronze")

    # Run dbt models to transform Bronze Tables to Silver then Gold
    dbt_transform =  EmptyOperator(task_id="dbt_transform")

    # Refresh PowerBI Semantic Model
    powerbi_refresh = EmptyOperator(task_id="powerbi_refresh")

    extract_taskgroup = extract_taskgroup()

    # Test comment

    start >> extract_taskgroup >> load_to_S3 >> copy_to_bronze >> dbt_transform >> powerbi_refresh >> end


    #new_task = EmptyOperator(task_id="new_task")
    #start >> extract_taskgroup >> load_to_S3 >> copy_to_bronze >> dbt_transform >> powerbi_refresh >> new_task >> end


finance_elt_dag()

