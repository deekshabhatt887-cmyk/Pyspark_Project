from pyspark.sql import SparkSession

from read_data import read_data

from validate_data import (validate_customers, 
    validate_duplicate_customers, 
    validate_duplicate_orders,
    validate_order_customers,
    validate_order_products
)

from transform_data import clean_customers, clean_orders

spark = SparkSession.builder \
    .appName("EcommerceDataPipeline") \
    .getOrCreate()

customers, products, orders = read_data(spark)

customers = clean_customers(customers)
orders = clean_orders(orders)

#customers.show()
orders.show()

validate_customers(customers)
validate_duplicate_customers(customers)
validate_duplicate_orders(orders)
validate_order_customers(orders, customers)
validate_order_products(orders, products)

print("Customers: ", customers.count())
print("Products: ", products.count())
print("Orders: ", orders.count())

