from pyspark.sql import SparkSession
from pyspark import SparkConf

# Generate SparkSession
def get_spark_session(_appName="Default AppName") -> SparkSession:
    
    # Create blank conf to get the configurations
    conf = SparkConf() \
        .setAppName(_appName)
    
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark