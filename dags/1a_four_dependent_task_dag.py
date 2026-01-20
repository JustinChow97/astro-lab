"""
## Real Use Case DAG

This dag pulls data from the FakeStoreAPI, lands the data into S3, copies into Snowflake, then transforms for consumption

The dag is as follows
1. Fetch_API_Data
2. Upload to S3
3. Copy into Snowflake
4. Transform

"""

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
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": duration(minutes=5),
    },
    tags=["etl", "demo"],
)
def simple_dag():

    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    # Fetches Data from the FakeStore API and returns as a JSON object. 
    @task
    def fetch_data():
        headers = {
                "User-Agent": "Mozilla/5.0 (compatible; Airflow/2.8)",
                "Accept": "application/json",
            }

        response = requests.get(
                #"https://fakestoreapi.com/products",
                "https://dummyjson.com/products",
                headers=headers,
                timeout=30,
            )
        response.raise_for_status()


        # url = "https://fakestoreapi.com/products"
        # response = requests.get(url)
        # response.raise_for_status()
        return(response.json())

    # Loads raw JSON list into S3 Bucket
    @task
    def load_to_S3(api_raw_json: list):
        #hook = S3Hook(aws_conn_id="aws_default") # Connection for local development

        hook = S3Hook(aws_conn_id="aws_s3_connection") # Connection for Astro Deployment
  
        # Convert JSON to String
        api_raw_string = json.dumps(api_raw_json, indent=2)

        # Create an S3 Hook, create folder structure and load and name file. 
        hook.load_string(
            string_data = api_raw_string,
            key="fakestore/product.json",
            bucket_name="astro-tech-assessment-jchow",
            replace=True,
        )

    # Perform Copy into from S3 External Stage to Raw Schema
    copy_products_to_raw = SnowflakeOperator(
        task_id = "copy_products_to_raw",
        #snowflake_conn_id="snowflake_default", # Connection for local dev 
        snowfake_conn_id="Snowflake_Connection" # Connection for Astro deploy
        
        sql =
        """
            USE SCHEMA BRONZE; 
            
            -- Clear table before inserting. 
            TRUNCATE TABLE IF EXISTS BRONZE.PRODUCTS;
            
            COPY INTO BRONZE.PRODUCTS
            from @s3_external_stage/fakestore/product.json
            FILE_FORMAT = 'MY_JSON_FORMAT'
            FORCE = TRUE

        """
        )
    
    # Flatten rows of JSON to  
    transform_products_raw = SnowflakeOperator(
        task_id = "transform_products_raw",
        snowflake_conn_id = "snowflake_default",
        sql = 
        """
        -- Clear table before inserting. 
        TRUNCATE TABLE IF EXISTS SILVER.PRODUCTS;

        -- Insert flattened data from raw. 
        INSERT INTO SILVER.PRODUCTS (ID, TITLE, PRICE, DESCRIPTION, CATEGORY, IMAGE)
        SELECT
            data:id::int AS id,
            data:title::string AS title,
            data:price::float AS price,
            data:description::string AS description,
            data:category::string AS category,
            data:image::string AS image
        FROM BRONZE.PRODUCTS;
        """
    )
    
    fetch_data = fetch_data()
    load_to_S3 = load_to_S3(fetch_data)

    start >> fetch_data >> load_to_S3 >> copy_products_to_raw >> transform_products_raw >> end

simple_dag()


# 1) extract_products_from_fakestore
# 2) upload_products_to_s3
# 3) load_products_to_raw
# 4) transform_raw_products_to_silver
# 5) aggregate_products_to_gold+