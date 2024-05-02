import unittest
from pyspark.sql import SparkSession
from pyspark_repo.src.assignment_5.util import *

class TestDataManipulation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder .appName("TestApp").getOrCreate()


    def test_create_dataframes(self):
        employee_df, department_df, country_df = create_dataframes(self.spark)
        self.assertIsNotNone(employee_df)
        self.assertIsNotNone(department_df)
        self.assertIsNotNone(country_df)

    def test_avg_salary_per_department(self):
        employee_df, _, _ = create_dataframes(self.spark)
        avg_salary_df = avg_salary_per_department(employee_df)
        self.assertIsNotNone(avg_salary_df)

    def test_employees_starting_with_m(self):
        employee_df, _, _ = create_dataframes(self.spark)
        employees_with_m = employees_starting_with_m(employee_df)
        self.assertIsNotNone(employees_with_m)

    def test_add_bonus_column(self):
        employee_df, _, _ = create_dataframes(self.spark)
        employee_df_with_bonus = add_bonus_column(employee_df)
        self.assertIsNotNone(employee_df_with_bonus)

    def test_reorder_columns(self):
        employee_df, _, _ = create_dataframes(self.spark)
        reordered_employee_df = reorder_columns(employee_df)
        self.assertIsNotNone(reordered_employee_df)

    def test_join_dataframes(self):
        employee_df, department_df, _ = create_dataframes(self.spark)
        join_types = ["inner", "left", "right"]
        for join_type in join_types:
            join_df = join_dataframes(employee_df, department_df, join_type)
            self.assertIsNotNone(join_df)

    def test_replace_state_with_country(self):
        employee_df, _, country_df = create_dataframes(self.spark)
        employee_df_with_country = replace_state_with_country(employee_df, country_df)
        self.assertIsNotNone(employee_df_with_country)

    def test_convert_to_lowercase(self):
        employee_df, _, country_df = create_dataframes(self.spark)
        employee_df_with_country = replace_state_with_country(employee_df, country_df)
        employee_df_lowercase = convert_to_lowercase(employee_df_with_country)
        self.assertIsNotNone(employee_df_lowercase)

if __name__ == "__main__":
    unittest.main()
#performed joins