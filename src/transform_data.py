from pyspark.sql.functions import trim, to_date

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