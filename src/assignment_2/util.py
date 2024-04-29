from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


# creating a dataset for card_number
def create_credit_card_dataframe(spark):
    data = [("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)]
    df= spark.createDataFrame(data, ["card_number"])
    return df

def get_num_partitions(df):
    num_partitions = df.rdd.getNumPartitions()
    return num_partitions
def increase_partition(df):
    num = int(input("enter the number of partition to be increase: "))
    increase=df.repartition(num)
    increase=increase.rdd.getNumPartitions()
    return increase
def back_to_original(df):
    decrease=df.coalesce(df.rdd.getNumPartitions())
    decrease=decrease.rdd.getNumPartitions()
    return decrease
def mask_credit_card_numbers(df):
    def mask_card_number(card_number):
        masked_number = '*' * 12 + card_number[-4:]
        return masked_number

    mask_card_number_udf = udf(mask_card_number, StringType())

    return df.withColumn("masked_card_number", mask_card_number_udf(col("card_number")))