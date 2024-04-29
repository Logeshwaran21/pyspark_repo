from pyspark_repo.src.assignment_2.util import *
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# creating a spark session
spark = SparkSession.builder.appName("Card_Number").getOrCreate()

# calling the  dataframe for credit card and calling the function to show the dataframe
df= create_credit_card_dataframe(spark)
logging.info("Calling the datafram to show the values",df)
df.show()

#print number of partitions
num_partitions = get_num_partitions(df)
logger.info("Number of partitions:",num_partitions)

# increase the number of partiton:
increase= increase_partition(df)
logger.info("number of partitions increased %d",increase)

#decrease the partition back to its original size
decrease = back_to_original(df)
logger.info("Partition back to its original size %d", decrease)

# Mask credit card numbers using UDF function
credit_card_df_masked = mask_credit_card_numbers(df)

# Show the masked credit card DataFrame
credit_card_df_masked.show()