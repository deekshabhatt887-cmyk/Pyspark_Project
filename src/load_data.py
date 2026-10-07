import os
from dotenv import load_dotenv

load_dotenv()


def save_orders_to_postgres(orders):

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    orders.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "processed_orders") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("Data successfully loaded into PostgreSQL!")