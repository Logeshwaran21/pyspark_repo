import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark_repo.src.assignment_3.util import *

class TestAssignment3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test") .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_custom_dataframe(self):
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
        expected_count = len(data)
        df = create_custom_dataframe(self.spark)
        self.assertEqual(df.count(), expected_count)

    def test_rename_columns(self):
        schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        data = [
            (1, 101, 'login', '2023-09-05 08:30:00')
        ]
        expected_columns = ['log_id', 'user_id', 'user_activity', 'time_stamp']
        df = self.spark.createDataFrame(data, schema)
        renamed_df = rename_columns(df)
        self.assertEqual(renamed_df.columns, expected_columns)

    def test_convert_to_timestamp(self):
        schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        data = [
            (1, 101, 'login', '2023-09-05 08:30:00')
        ]
        df = self.spark.createDataFrame(data, schema)
        timestamp_df = convert_to_timestamp(df)
        self.assertEqual(timestamp_df.select("time_stamp").first()[0], '2023-09-05 08:30:00')

    def test_count_actions_last_7_days(self):
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
        df = self.spark.createDataFrame(data, schema)
        expected_count = 2
        df_last_7_days = count_actions_last_7_days(df)
        self.assertEqual(df_last_7_days.count(), expected_count)

    def test_convert_to_login_date(self):
        schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        data = [
            (1, 101, 'login', '2023-09-05 08:30:00')
        ]
        df = self.spark.createDataFrame(data, schema)
        login_date_df = convert_to_login_date(df)
        self.assertEqual(login_date_df.select("login_date").first()[0], '2023-09-05')

if __name__ == '__main__':
    unittest.main()
#main function