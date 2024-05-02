import unittest
from pyspark.sql import SparkSession
from pyspark_repo.src.assignment_2.util import *


class TestCreditCardDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing df
        cls.spark = SparkSession.builder.appName("TestCreditCardDataFrame").getOrCreate()
    @classmethod


    def test_create_credit_card_dataframe(self):
        data = [("1234567891234567",),
                ("5678912345671234",),
                ("9123456712345678",),
                ("1234567812341122",),
                ("1234567812341342",)]

        expected_count = 5

        # Create DataFrame
        df = create_credit_card_dataframe(self.spark)

        # Check if DataFrame is created correctly
        self.assertEqual(df.count(), expected_count)

    def test_get_num_partitions(self):
        data = [("1234567891234567",),
                ("5678912345671234",),
                ("9123456712345678",),
                ("1234567812341122",),
                ("1234567812341342",)]

        expected_partitions = 1

        # Create DataFrame
        df = create_credit_card_dataframe(self.spark)

        # Check number of partitions
        num_partitions = get_num_partitions(df)

        # Check if number of partitions is correct
        self.assertEqual(num_partitions, expected_partitions)

    def test_increase_partitions(self):
        data = [("1234567891234567",),
                ("5678912345671234",),
                ("9123456712345678",),
                ("1234567812341122",),
                ("1234567812341342",)]

        expected_partitions = 2

        # Create DataFrame
        df = create_credit_card_dataframe(self.spark)

        # Increase partitions
        df = increase_partition(df, 2)  # specify the number of partitions

        # Check number of partitions
        num_partitions = get_num_partitions(df)

        # Check if number of partitions is increased
        self.assertEqual(num_partitions, expected_partitions)

    def test_decrease_partitions(self):
        data = [("1234567891234567",),
                ("5678912345671234",),
                ("9123456712345678",),
                ("1234567812341122",),
                ("1234567812341342",)]

        expected_partitions = 1

        # Create DataFrame
        df = create_credit_card_dataframe(self.spark)

        # Increase partitions first
        df = increase_partition(df)  # specify the number of partitions

        # Decrease partitions
        df = back_to_original(df)  # specify the number of partitions

        # Check number of partitions
        num_partitions = get_num_partitions(df)

        # Check if number of partitions is decreased back to 1
        self.assertEqual(num_partitions, expected_partitions)

    def test_mask_credit_card_numbers(self):
        data = [("1234567891234567",),
                ("5678912345671234",),
                ("9123456712345678",),
                ("1234567812341122",),
                ("1234567812341342",)]

        expected_masked_numbers = ['************4567', '************1234', '************5678', '************1122', '************1342']

        # Create DataFrame
        df = create_credit_card_dataframe(self.spark)

        # Mask credit card numbers
        masked_df = mask_credit_card_numbers(df)

        # Collect masked numbers
        actual_masked_numbers = masked_df.select("masked_card_number").rdd.flatMap(lambda x: x).collect()

        # Check if masked numbers are correct
        self.assertEqual(actual_masked_numbers, expected_masked_numbers)


if __name__ == "__main__":
    unittest.main()