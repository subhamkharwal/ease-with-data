from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame, udf
from pyspark.sql.types import StringType
import json

# Date Utility to generate Date source data for next 1 year
def date_data(start_run_dt: str = '20230101', num_years: int = 1) -> list:
    _data = []
    _start_date = datetime.strptime(start_run_dt, '%Y%m%d')
    _data.append([
        datetime.strftime(_start_date, '%Y-%m-%d'), 
        datetime.strftime(_start_date, '%d'),
        datetime.strftime(_start_date, '%m'),
        datetime.strftime(_start_date, '%Y'),
        datetime.strftime(_start_date, '%A')])
    _next_date = _start_date
    for i in range(0, num_years*364):
        _next_date = _next_date + timedelta(days = 1)
        _data.append([
        datetime.strftime(_next_date, '%Y-%m-%d'), 
        datetime.strftime(_next_date, '%d'),
        datetime.strftime(_next_date, '%m'),
        datetime.strftime(_next_date, '%Y'),
        datetime.strftime(_next_date, '%A')])
        
    return _data

# Cast all dataframe cols to string and return col name in list
def get_string_cols(spark: SparkSession, df: DataFrame) -> list:
    _col_list = []
    for col in df.columns:
        _col_list.append(f"cast({col} as string) as {col}")
    return _col_list

# Get rundate from config
def get_rundate() -> str:
    try:
        with open("run_config.txt", "r") as f:
            data = json.load(f)
            f.close()
        return data['rundate']
    except Exception as e:
        print(e)
        return "19000101"

