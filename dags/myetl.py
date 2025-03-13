from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
import os

def pause_dags():
    os.system("airflow dags pause myetl")

def converter_pq(dis_path):
    import pandas as pd
    import pyarrow.parquet as pq
    import os
    base_path ="/home/jacob/data/"
    csv_file = f"{base_path}{dis_path}/data.csv"
    parquet_file = f"{base_path}{dis_path}/data.parquet"
    
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV 파일이 아래 주소에 존재하지 않습니다: {csv_file}")
    
    df = pd.read_csv(csv_file)
    try:
        df.to_parquet(parquet_file, engine="pyarrow")
        return f"Parquet 파일이 아래 주소에 생성되었습니다: {parquet_file}"
    except Exception:
        return "파일 생성 중 오류가 발생했습니다"
        

def converter_agg(dis_path):
    import pandas as pd
    import pyarrow.parquet as pq
    import os
    base_path ="/home/jacob/data/"
    parquet_file = f"{base_path}{dis_path}/data.parquet"
    agg_file = f"{base_path}{dis_path}/agg.csv"
    
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Parquet파일이 아래 주소에 존재하지 않습니다: {parquet_file}")
    
    df = pd.read_parquet(parquet_file, engine='pyarrow')
    group_df = df.groupby(["value"]).count().reset_index()
    try:
        group_df.to_csv(agg_file, index=False)
        return f" CSV 파일이 아래 경로에 생성되었습니다: {agg_file}"
    except Exception:
        return "파일 생성 중 오류가 발생했습니다"

with DAG(
    "myetl",
    #schedule=timedelta(days=1),
    #schedule="* * * * * *",
    schedule="@hourly",
    start_date=pendulum.datetime(2025,3,12, tz="Asia/Seoul"),
    catchup=False,
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    end = PythonOperator(task_id="end",
                       python_callable= pause_dags)
    
    make_data = BashOperator(
                task_id="mk_data",
                bash_command=
                "bash /home/jacob/airflow/make_data.sh /home/jacob/data/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"
                )
    
    load_data = PythonVirtualenvOperator(task_id="load_data",
                                            python_callable= converter_pq,
                                            requirements=["pands","pyarrow"],
                                            op_kwargs={"dis_path":"{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
                                            )
    
    agg_data = PythonVirtualenvOperator(task_id="agg_data",
                                            python_callable= converter_agg,
                                            requirements=["pands","pyarrow"],
                                            op_kwargs={"dis_path":"{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
                                            )
     
    start >> make_data >> load_data >> agg_data >> end
if __name__ == "__main__":
    dag.test()