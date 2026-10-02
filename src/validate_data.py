def validate_customers(customers):

    print("Checking customer data....")

    customers.filter(
        customers["customer_id"].isNull()
    ).show()

def validate_duplicate_customers(customers):

    print("Checking for duplicates customer IDs....")

    customers.groupBy("customer_id")\
    .count()\
    .filter("count > 1")\
    .show()

def validate_duplicate_orders(orders):

    print("Checking for duplicate order IDs....")

    orders.groupBy("order_id")\
    .count()\
    .filter("count > 1")\
    .show()

def validate_order_customers(orders, customers):

    print("Checking customer Ids in orders....")

    invalid_orders = orders.join(
        customers,
        orders["customer_id"] == customers["customer_id"],
        "left_anti"
    )

    invalid_orders.show()

def validate_order_products(orders, products):

    print("Checking product IDs in orders...")
    invalid_orders = orders.join(
        products,
        orders["product_id"] == products["product_id"],
        "left_anti"
    )

    invalid_orders.show()