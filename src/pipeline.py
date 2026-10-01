from pyspark.sql import SparkSession

from read_data import read_data

from validate_data import validate_record

spark = SparkSession.builder \
    .appName("EcommerceDataPipeline") \
    .getOrCreate()

customers, products, orders = read_data(spark)

print("Customers: ", customers.count())
print("Products: ", products.count())
print("Orders: ", orders.count())

validate_record(
    customers,
    products,
    orders
)