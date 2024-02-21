"""_summary_
    This is where spark jobs will be written to listen 
    to events from kafka.
"""
from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum

def read_kafka_topic(spark, topic, schema):
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes')
        )

def main():
    """
    ------ Steps to setting up Spark ------
    (1) Create spark session & setup configurations connections 
    -   Adjust Log level to minimize the console output on executors (Optional)
    (2) Setup Schema to be used to deserialize data coming from source e.g. Kafka
    (3) Consume/read data from source including the schema to be used
    (4) Process/transform the data 
    (5) Write the processed data to the destination
    """
    
    
    # ---------------- Creating the SparkSession ---------------- 
    # NB: Get config from mvnrepository.com "<groupid>:<artifactId>:<version>"
    
    """_summary_
        Jar Files used [Establishes a relationship between Kafka, Hadoop, AWS & Spark]
        - (sqlkafka) enables spark connect directly to Kafka.
        - (Haddop AWS) enables connection from spark to AWS
        - (AWS JAVA SDK) enables connection from spark to AWS
        2nd config tells Hadoop & spark how to connect to S3 bucket
    """
    spark = (SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.package", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
                "org.apache.hadoop:hadoop-aws:3.3.1",
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider")\
        .getOrCreate())
    
    # Adjust Log level to minimize the console output on executors (Optional) 
    spark.sparkContext.setLogLevel('WARN')
    
    # ---------------- Creating the Schemas ---------------- 
    # Vehicle Schema
    vehicleSchema =  StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuelType', StringType(), True)
    ])
    
    # GPS Schema
    gpsSchema =  StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('vehicleType', StringType(), True)
    ])
    
    # Traffic Camera Schema
    trafficCameraSchema =  StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('cameraId', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),
        StructField('snapshot', StringType(), True)
    ])
    
    # Weather Schema
    weatherSchema =  StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('weatherCondition', StringType(), True),
        StructField('precipitation', DoubleType(), True),
        StructField('windSpeed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('airQualityIndex', DoubleType(), True)
    ])
    
    # Emergency Schema
    emergencySchema =  StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),
        StructField('incidentType', StringType(), True),
        StructField('type', StringType(), True),
        StructField('location', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('status', StringType(), True),
        StructField('description', StringType(), True)
    ])


# ---------------- Consuming/Reading the data from Kafka ---------------- 

    vehicle_df = read_kafka_topic(spark, "vehicle_data", vehicleSchema).alias('vehicle')
    gps_df = read_kafka_topic(spark, "gps_data", gpsSchema).alias('gps')
    traffic_df = read_kafka_topic(spark, "traffic_data", trafficCameraSchema).alias('traffic')
    weather_df = read_kafka_topic(spark, "weather_data", weatherSchema).alias('weather')
    emergency_df = read_kafka_topic(spark, "emergency_data", emergencySchema).alias('emergency')
    
    # Join all the DFs with the ID & Timestamps
    


if __name__ == "__main__":
    main()