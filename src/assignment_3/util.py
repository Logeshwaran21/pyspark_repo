from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import col, count, expr, datediff
from datetime import datetime

def create_custom_dataframe(spark):
    schema = StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    return spark.createDataFrame(data, schema)

def rename_columns(df):
    return df.select(
        col("log_id").alias("log_id"),
        col("user_id").alias("user_id"),
        col("action").alias("user_activity"),
        col("timestamp").alias("time_stamp")
    )

def convert_to_timestamp(df):
    return df.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

def count_actions_last_7_days(df):
    return df.filter(datediff(expr("date('2023-09-05')"), expr("date(time_stamp)")) <= 7).groupBy("user_id").agg(count("*").alias("num_actions_last_7_days"))

def convert_to_login_date(df):
    return df.withColumn("login_date", df["time_stamp"].cast(DateType()))