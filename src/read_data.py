def read_data(spark):

 customers = spark.read.csv(
    "data/raw/customers.csv",
    header=True,
    inferSchema=True
)

 products = spark.read.csv(
    "data/raw/products.csv",
    header=True,
    inferSchema=True
)

 orders = spark.read.csv(
    "data/raw/orders.csv",
    header=True,
    inferSchema=True
)

 return customers, products, orders

