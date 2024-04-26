import logging
from pyspark.sql import SparkSession

# Import utility functions
from util import create_purchase_data_df, create_product_data_df, find_customers_with_only_iphone13, \
    find_customers_upgraded_to_iphone14, find_customers_bought_all_products

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession
spark = SparkSession.builder.appName("Customer Product Purchase").getOrCreate()

# Create purchase_data_df and product_data_df
purchase_data_df = create_purchase_data_df(spark)
product_data_df = create_product_data_df(spark)

# Find customers who bought only iphone13
logger.info("Customers who bought only iphone13:")
find_customers_with_only_iphone13(purchase_data_df, logger)

# Find customers who upgraded from iphone13 to iphone14
logger.info("Customers who upgraded from iphone13 to iphone14:")
find_customers_upgraded_to_iphone14(purchase_data_df, logger)

# Find customers who bought all models in the new Product Data
logger.info("Customers who bought all products:")
find_customers_bought_all_products(purchase_data_df, product_data_df, logger)