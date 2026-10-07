from pyspark.sql import SparkSession

from read_data import read_data

from validate_data import (validate_customers, 
    validate_duplicate_customers, 
    validate_duplicate_orders,
    validate_order_customers,
    validate_order_products
)

from transform_data import clean_customers, clean_orders, calculate_order_amount, select_order_columns

from analytics import calculate_total_sales, calculate_sales_by_product, calculate_sales_by_city, calculate_monthly_sales, calculate_top_customers

from load_data import save_orders_to_postgres

# spark = SparkSession.builder \
#     .appName("EcommerceDataPipeline") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("EcommerceDataPipeline") \
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8"
    ) \
    .getOrCreate()


customers, products, orders = read_data(spark)

customers = clean_customers(customers)
orders = clean_orders(orders)
orders = calculate_order_amount(orders, products)
orders = select_order_columns(orders)

print("Orders before PostgreSQL load:")
orders.show()

save_orders_to_postgres(orders)
#save_orders(orders)
total_sales = calculate_total_sales(orders)
sales_by_city = calculate_sales_by_city(orders, customers)
sales_by_product = calculate_sales_by_product(orders, products)
monthly_sales = calculate_monthly_sales(orders)
top_customers =  calculate_top_customers(orders, customers)


customers.show()
orders.show()
total_sales.show()
sales_by_city.show()
sales_by_product.show()
monthly_sales.show()
top_customers.show()
 
validate_customers(customers)
validate_duplicate_customers(customers)
validate_duplicate_orders(orders)
validate_order_customers(orders, customers)
validate_order_products(orders, products)

print("Customers: ", customers.count())
print("Products: ", products.count())
print("Orders: ", orders.count())

test_df = spark.createDataFrame(
    [(1, "test")],
    ["id", "name"]
)

test_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pyspark_retail") \
    .option("dbtable", "spark_test") \
    .option("user", "postgres") \
    .option("password", "Deeksha@123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Test table written!")

