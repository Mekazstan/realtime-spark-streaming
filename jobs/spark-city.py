"""_summary_
    This is where spark jobs will be written to listen 
    to events from kafka.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum

def main():
    # Creating the SparkSession
    spark = SparkSession.builder.appName("SmartCityStreaming") \
    .config('spark.jars.package', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0') \
    .getOrCreate()


if __name__ == "__main__":
    main()