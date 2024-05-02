
from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_dataframes(spark):
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("age", IntegerType(), True)
    ])
    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    employee_data = [
        (11, "james", "D101", "ny", 9000.0, 34),
        (12, "michel", "D101", "ny", 8900.0, 32),
        (13, "robert", "D102", "ca", 7900.0, 29),
        (14, "scott", "D103", "ca", 8000.0, 36),
        (15, "jen", "D102", "ny", 9500.0, 38),
        (16, "jeff", "D103", "uk", 9100.0, 35),
        (17, "maria", "D101", "ny", 7900.0, 40)
    ]

    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    employee_df = spark.createDataFrame(employee_data, schema=employee_schema)
    department_df = spark.createDataFrame(department_data, schema=department_schema)
    country_df = spark.createDataFrame(country_data, schema=country_schema)

    return employee_df, department_df, country_df

def avg_salary_per_department(employee_df):
    avg_salary_df = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    return avg_salary_df

def employees_starting_with_m(employee_df):
    employees_with_m = employee_df.filter(col("employee_name").startswith("m")).select("employee_name", "department")
    return employees_with_m

def add_bonus_column(employee_df):
    employee_df_with_bonus = employee_df.withColumn("bonus", col("salary") * 2)
    return employee_df_with_bonus

def reorder_columns(employee_df):
    reordered_employee_df = employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")
    return reordered_employee_df

def join_dataframes(employee_df, department_df, join_type):
    join_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, join_type)
    return join_df

def replace_state_with_country(employee_df, country_df):
    employee_df_with_country = employee_df.join(country_df, employee_df.state == country_df.country_code, "left").drop("state").withColumnRenamed("country_name", "state")
    return employee_df_with_country

def convert_to_lowercase(employee_df):
    new_column_names = [col.lower() for col in employee_df.columns]
    return employee_df.toDF(*new_column_names).withColumn("load_date", current_date())