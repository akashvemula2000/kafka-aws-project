from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json
from config import configuration
import os


spark = SparkSession.builder \
        .appName("SmartCityStreaming")\
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.6,"
                "org.apache.hadoop:hadoop-aws:3.4.1,"
                "com.amazonaws:aws-java-sdk:1.12.785") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.security.authentication", "simple")\
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .getOrCreate()


vehicle_schema = StructType([
    StructField("id", StringType()),  # UUID as string
    StructField("deviceId", StringType()),
    StructField("timestamp", TimestampType()),  # ISO string
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    StructField("speed", IntegerType()),
    StructField("direction", StringType()),
    StructField("make", StringType()),
    StructField("model", StringType()),
    StructField("year", IntegerType()),
    StructField("fueltype", StringType())
])


gps_schema = StructType([
    StructField("id", StringType()),          
    StructField("deviceId", StringType()),
    StructField("timestamp", TimestampType()),  
    StructField("speed", IntegerType()),       
    StructField("direction", StringType()),
    StructField("vehicle_type", StringType())
])

traffic_camera_schema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),  # latitude
        StructField("longitude", DoubleType())   # longitude
    ])),
    StructField("cameraId", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("snapshot", StringType())
])

weather_schema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    StructField("timestamp", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("weather_condition", StringType()),
    StructField("windSpeed", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("precipitation", DoubleType()),
    StructField("AQI", DoubleType())
])

emergency_schema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("incidentId", StringType()),
    StructField("type", StringType()),
    StructField("location", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])),
    StructField("timestamp", TimestampType()),
    StructField("status", StringType())
])

def read_from_kafka(topic,schema):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
        .option("subscribe",topic)\
        .option("startingOffsets","earliest")\
        .load()\
        .selectExpr('CAST(value as STRING) as json_str')\
        .withColumn("data",from_json("json_str",schema))\
        .select("data.*")

    return df    

vehicle_df = read_from_kafka("vehicle_data",vehicle_schema)
gps_df = read_from_kafka("gps_data",gps_schema)
traffic_df = read_from_kafka("traffic_data",traffic_camera_schema)
weather_df = read_from_kafka("weather_data",weather_schema)
emergency_df = read_from_kafka("emergency_data",emergency_schema)

# vehicle_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start() \
#     .awaitTermination()

def stream_writer(data,checkpoint, output):
    return data.writeStream\
    .format("parquet")\
    .option("checkpointLocation", checkpoint)\
    .option("path", output)\
    .outputMode("append")\
    .start()
    
    

q1 = stream_writer(
    vehicle_df,
    "s3a://kafka-iot-project/checkpoints/vehicle_data/",
    "s3a://kafka-iot-project/data/vehicle_data/"
)

q2 = stream_writer(
    gps_df,
    "s3a://kafka-iot-project/checkpoints/gps_data/",
    "s3a://kafka-iot-project/data/gps_data/"
)

q3 = stream_writer(
    traffic_df,
    "s3a://kafka-iot-project/checkpoints/traffic_data/",
    "s3a://kafka-iot-project/data/traffic_data/"
)

q4 = stream_writer(
    weather_df,
    "s3a://kafka-iot-project/checkpoints/weather_data/",
    "s3a://kafka-iot-project/data/weather_data/"
)

q5 = stream_writer(
    emergency_df,
    "s3a://kafka-iot-project/checkpoints/emergency_data/",
    "s3a://kafka-iot-project/data/emergency_data/"
)


spark.streams.awaitAnyTermination()