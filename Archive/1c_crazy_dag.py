from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime, duration
from airflow.sdk import chain, chain_linear

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": duration(minutes=5),
    },
    tags=["etl", "demo"],
    )
def crazy_dag():
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

# Tasks
    t1 = EmptyOperator(task_id = "t1")
    t2 = EmptyOperator(task_id = "t2")
    t3 = EmptyOperator(task_id = "t3")
    t4 = EmptyOperator(task_id = "t4")
    t5 = EmptyOperator(task_id = "t5")
    t6 = EmptyOperator(task_id = "t6")
    t7 = EmptyOperator(task_id = "t7")
    t8 = EmptyOperator(task_id = "t8")
    t9 = EmptyOperator(task_id = "t9")
    t10 = EmptyOperator(task_id = "t10")


# Task Lists
    task_list_a = []
    task_list_b = []

    for index in range(20):
        if index % 2 == 0:
            task = EmptyOperator(task_id=f"ta_{index}")
            task_list_a.append(task)

        else:
            task = EmptyOperator(task_id=f"tb_{index}")
            task_list_b.append(task)

# Task Group
    @task_group(
        group_id="group1"
    )
    def tg1():
        
        @task_group(
            group_id="group1_inner"
        )
        def tg2_inner():
            task_list_c = []
            task_list_d = []
            for index in range(10):
                if index % 2 == 0:
                    task = EmptyOperator(task_id=f"tc_{index}")
                    task_list_c.append(task)
                else:
                    task = EmptyOperator(task_id=f"tc_{index}")
                    task_list_d.append(task)
            chain_linear(task_list_c, task_list_d)
            
        tg2_inner = tg2_inner()     

        tc_1 = EmptyOperator(task_id="tc_1")
        tc_2 = EmptyOperator(task_id="tc_2")
        tc_3 = EmptyOperator(task_id="tc_3")
        tc_4 = EmptyOperator(task_id="tc_4")
        tc_5 = EmptyOperator(task_id="tc_5")

        chain_linear(tc_1, [tc_2, tc_3, tg2_inner, tc_4, tc_5])

    tg1 = tg1()



   #  start >> t1 >> [t2,t3,t4] >> chain(t5,[t6,t7]) >> end
    chain_linear (start, t1, [t2,t3,t4], tg1, task_list_a, [t5,t6,t7,t8,t9], task_list_b, t10, end)

crazy_dag()