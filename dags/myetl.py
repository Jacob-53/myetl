from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator


with DAG(
    "myetl",
    #schedule=timedelta(days=1),
    #schedule="* * * * * *",
    schedule="@hourly",
    start_date=pendulum.datetime(2025,3,13, tz="Asia/Seoul"),
    catchup=True,
    max_active_runs=1
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
                       
    make_data = BashOperator(
                task_id="mk_data",
                bash_command=
                "bash /home/jacob/airflow/make_data.sh /home/jacob/data/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"
                )
    
    def f_converter_pq(dis_path):
        from myetl.myetl_db import converter_pq
        return converter_pq(dis_path)
    
    def f_converter_agg(dis_path):
        from myetl.myetl_db import converter_agg
        return converter_agg(dis_path)
    
    load_data = PythonVirtualenvOperator(task_id="load_data",
                                            python_callable= f_converter_pq,
                                            requirements=["git+https://github.com/Jacob-53/myetl.git@0.1.0"],
                                            op_kwargs={"dis_path":"{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
                                            )
    
    agg_data = PythonVirtualenvOperator(task_id="agg_data",
                                            python_callable= f_converter_agg,
                                            requirements=["git+https://github.com/Jacob-53/myetl.git@0.1.0"],
                                            op_kwargs={"dis_path":"{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
                                            )
     
    start >> make_data >> load_data >> agg_data >> end
if __name__ == "__main__":
    dag.test()