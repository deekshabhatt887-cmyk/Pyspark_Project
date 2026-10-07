from pyspark.sql.functions import sum, month

def calculate_total_sales(orders):

    total_sales =  orders.select(
        sum("total_amount").alias("total_sales")
    )

    return   total_sales

def calculate_sales_by_city(orders, customers):

    sales_by_city = orders.join(
        customers,
        orders["customer_id"] == customers["customer_id"],
        "inner"
    )

    sales_by_city = sales_by_city.groupBy(
        "city"
    ).agg(
        sum("total_amount").alias("total_sales")    
    )

    return sales_by_city

def calculate_sales_by_product(orders, products):

    sales_by_product = orders.join(
        products,
        orders["product_id"] == products["product_id"],
        "inner"
    )

    sales_by_product = sales_by_product.groupBy(
        "product_name"
    ).agg(
        sum("total_amount").alias("total_sales")
    )

    return sales_by_product

def calculate_monthly_sales(orders):

    monthly_sales = orders.withColumn(
        "month",
        month("order_date")
    )

    monthly_sales = monthly_sales.groupBy(
        "month"
    ).agg(
        sum("total_amount").alias("total_sales")
    )

    return monthly_sales

def calculate_top_customers(orders, customers):

    customers_for_join = customers.select(
        "customer_id",
        "name"
    )

    top_customers = orders.join(
        customers_for_join,
        orders["customer_id"] == customers_for_join["customer_id"],
        "inner"
    )

    top_customers = top_customers.select(
        orders["customer_id"],
        customers_for_join["name"],
        orders["total_amount"]
    )

    top_customers = top_customers.groupBy(
        "customer_id",
        "name"
    ).agg(
        sum("total_amount").alias("total_sales")
    )

    top_customers = top_customers.orderBy(
        "total_sales",
        ascending=False
    )

    return top_customers