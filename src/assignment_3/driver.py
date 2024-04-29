import logging
from pyspark.sql import SparkSession
from util import *

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SparkSession
spark = SparkSession.builder.appName("User Activity Analysis").getOrCreate()

# Create DataFrame with custom schema
df = create_custom_dataframe(spark)

# Rename the column names
df = rename_columns(df)

# Convert timestamp column to timestamp type
df = convert_to_timestamp(df)

# Print schema to verify the timestamp column type
logger.info("DataFrame Schema:")
df.printSchema()

# Calculate the number of actions performed by each user in the last 7 days
logger.info("Number of actions performed by each user in the last 7 days:")
df_last_7_days = count_actions_last_7_days(df)
df_last_7_days.show()

# Convert timestamp column to login_date column with YYYY-MM-DD format
df_login_date = convert_to_login_date(df)
df_login_date.show()

spark.stop()