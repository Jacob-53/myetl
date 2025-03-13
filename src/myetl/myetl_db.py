import pandas as pd
import pyarrow.parquet as pq
import os

def converter_agg(dis_path):
    
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
    

def converter_pq(dis_path):
   
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