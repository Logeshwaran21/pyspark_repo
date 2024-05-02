import logging
from pyspark.sql import SparkSession
from util import *

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SparkSession
spark = SparkSession.builder.appName("Data Manipulation").getOrCreate()

# Create DataFrames
employee_df, department_df, country_df = create_dataframes(spark)

# Question 2: Find avg salary of each department
logger.info("Average salary of each department:")
avg_salary_df = avg_salary_per_department(employee_df)
avg_salary_df.show()

# Question 3: Find the employee’s name and department name whose name starts with ‘m’
logger.info("Employees whose names start with 'm':")
employees_with_m = employees_starting_with_m(employee_df)
employees_with_m.show()

# Question 4: Create another new column in employee_df as a bonus by multiplying employee salary * 2
logger.info("Employee DataFrame with bonus column:")
employee_df_with_bonus = add_bonus_column(employee_df)
employee_df_with_bonus.show()

# Question 5: Reorder the column names of employee_df
logger.info("Reordered Employee DataFrame:")
reordered_employee_df = reorder_columns(employee_df)
reordered_employee_df.show()

# Question 6: Give the result of an inner join, left join, and right join when joining employee_df with department_df
logger.info("Inner join result:")
inner_join_result = join_dataframes(employee_df, department_df, "inner")
inner_join_result.show()

logger.info("Left join result:")
left_join_result = join_dataframes(employee_df, department_df, "left")
left_join_result.show()

logger.info("Right join result:")
right_join_result = join_dataframes(employee_df, department_df, "right")
right_join_result.show()

# Question 7: Derive a new data frame with country_name instead of State in employee_df
logger.info("Employee DataFrame with country instead of state:")
employee_df_with_country = replace_state_with_country(employee_df, country_df)
employee_df_with_country.show()

# Question 8: Convert all the column names into lowercase from the result of question 7 and add the load_date column
logger.info("Employee DataFrame with lowercase column names and load_date column:")
employee_df_lowercase = convert_to_lowercase(employee_df_with_country)
employee_df_lowercase.show()

# Stop SparkSession
spark.stop()