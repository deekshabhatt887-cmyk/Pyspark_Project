from pyspark.sql.functions import trim, to_date, col

def clean_customers(customers):

    customers = customers.withColumn(
        "city",
        trim(customers["city"])

    )

    customers = customers.withColumn(
        "name",
        trim(customers["name"])
    )

    customers = customers.withColumn(
        "state",
        trim(customers["state"])
    )


    return customers

def clean_orders(orders):

    orders = orders.withColumn(
        "order_date",
        to_date(orders["order_date"], "yyyy-MM-dd")
    )

    return orders

def calculate_order_amount(orders, products):

    products_for_join = products.select(
        col("product_id").alias("product_key"),
        col("price")
    )

    orders = orders.join(
        products_for_join,
        orders["product_id"] == products_for_join["product_key"],
        "inner"
    )

    orders = orders.withColumn(
        "total_amount",
        orders["quantity"] * orders["price"]
    )

    orders = orders.drop("product_key")

    return orders
def select_order_columns(orders):

    orders = orders.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_date",
        "price",
        "total_amount"
    )

    return orders