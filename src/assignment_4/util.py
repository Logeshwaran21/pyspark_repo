from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Function to create a Spark session
def spark_session():
    spark = SparkSession.builder.appName("spark-assignment").getOrCreate()
    return spark

# Function to read JSON file
def read_json(spark, path):
    df = spark.read.json(path, multiLine=True)
    return df

# Function to flatten the DataFrame
def flatten_df(df):
    flattened_df = df.select("*", explode("employees").alias("employee")).selectExpr("*", "employee.*", "properties.*").drop("employees", "employee", "properties")
    return flattened_df

# Function to print record count before and after flattening
def count_before_after_flatten(df, flattened_df):
    print("\nBefore Flatten: ", df.count())
    print("\nAfter Flatten: ", flattened_df.count())

# Function to differentiate the difference using explode, explode outer, posexplode functions
def diff_explode_outer_posexplode(spark):
    data = [
        (1, [1, 2, 3]),
        (2, [4, None, 6]),
        (3, [])
    ]
    df = spark.createDataFrame(data, ["id", "numbers"])
    print("Original DataFrame:")
    df.show()
    exploded_df = df.select("id", explode("numbers").alias("number"))
    print("Exploded DataFrame:")
    exploded_df.show()
    exploded_outer_df = df.select("id", explode_outer("numbers").alias("number"))
    print("Exploded Outer DataFrame:")
    exploded_outer_df.show()
    pos_exploded_df = df.select("id", posexplode("numbers").alias("pos", "number"))
    print("PosExploded DataFrame:")
    pos_exploded_df.show()

# Function to filter the DataFrame for empId == 1001
def filter_employee_with_id(df, id):
    return df.filter(df['empId'] == id)

# Function to convert column names from camel case to snake case
def to_snake_case(dataframe):
    snake_case_df = dataframe.toDF(*(col.lower() for col in dataframe.columns))
    return snake_case_df

# Function to add a new column named load_date with the current date
def add_load_date_with_current_date(df):
    result = df.withColumn("load_date", current_date())
    return result

# Function to create 3 new columns as year, month, and day from the load_date column
def add_year_month_day(df):
    year_month_day_df = df.withColumn("year", year(df.load_date)) .withColumn("month", month(df.load_date)) .withColumn("day", day(df.load_date))
    return year_month_day_df