from airflow.sdk import dag, task
from pendulum import datetime, duration




@dag(
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup = False,
    doc_md=__doc__,
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": duration(minutes=5),
    },
    tags=["etl", "demo"],
)


def parallel_dag():

    @task
    def extract_sales_data():
        return ("Successfully extracted Sales Data from Netsuite API.")

    @task
    def extract_budget_data():
        return ("Successfully ext   racted Budgeting Data from Quickbooks API.")

    @task
    def extract_people_data():
        return ("Succescfully extracted People Data")
    
    


    extract_sales_data()
    extract_budget_data()
    extract_people_data()

parallel_dag()
