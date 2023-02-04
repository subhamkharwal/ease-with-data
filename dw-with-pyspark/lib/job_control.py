from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, max
from pyspark.sql.types import TimestampType
from delta import DeltaTable

# Insert log in job_control table 
def insert_log(spark: SparkSession, schema_name: str, table_name: str, max_timestamp:str) -> bool:
    try:
        _data = [
            [schema_name, table_name, max_timestamp]
        ]
        _cols = ["schema_name", "table_name", "max_timestamp"]
        
        # Create Dataframe
        df_raw = spark.createDataFrame(data = _data, schema = _cols)
        
        # Transform the timestamps
        df_processed = df_raw \
            .withColumn("max_timestamp", to_timestamp(lit(max_timestamp))) \
            .withColumn("insert_dt", current_timestamp())
        
        # Write into JOB_CONTROL table
        df_processed.write.format("delta").mode("append").saveAsTable("edw.job_control")
        return True
    except Exception as e:
        return False
    

# Get the max_timestamp for a table
def get_max_timestamp(spark: SparkSession, schema_name: str, table_name: str) -> str:
    try:
        # Read the job control table
        df = spark.read.table("edw.job_control")
        
        # Get the max_timestamp, if null then return default low value
        df_max_timestamp = df.where(f"schema_name = '{schema_name}' and table_name = '{table_name}'") \
            .groupBy("schema_name", "table_name").agg(max("max_timestamp").alias("max_timestamp"))
        
        # Check if data present or not
        if df_max_timestamp.count() > 0: 
            return str(df_max_timestamp.take(1)[0][2])
        else:
            return "1900-01-01 00:00:00.000000"
    except Exception as e:
        return None
    

# Remove table data for full loads    
def delete_log(spark: SparkSession, schema_name: str, table_name: str) -> bool:
    try:
        # Read Delta table
        dt = DeltaTable.forName(spark, "edw.job_control")
        
        # delete table data
        dt.delete(f"schema_name = '{schema_name}' and table_name = '{table_name}'")
        
        return True
    except Exception as e:
        return False
    
# Trunccate the logs table completely
def truncate_logs(spark: SparkSession, schema_name: str, table_name: str) -> bool:
    try:
        # Read Delta table
        dt = DeltaTable.forName(spark, "edw.job_control")
        
        # delete table data and vaccumm to 0
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
        dt.delete(f"1=1")
        dt.vacuum(0)

        return True
    except Exception as e:
        return False
    
if __name__ == '__main__':
    insert_log(spark, "edw_ld", "dim_store_stg", "2023-01-01 00:00:00.000")
    get_max_timestamp(spark, "edw_ld", "dim_store_stg")
    delete_log(spark, "edw_ld", "dim_store_stg")
    truncate_logs(spark, "edw_ld", "dim_store_stg")