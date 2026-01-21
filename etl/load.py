# etl/load.py

import psycopg2
import pandas as pd
from transform import transform_data

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="sales_dw",
        user="postgres",
        password="postgres"
    )


def load_dim_customer(cursor, df):
    customers = df[['customer_id', 'customer_name', 'segment']].drop_duplicates()
    for _, row in customers.iterrows():
        cursor.execute("""
            INSERT INTO dim_customer (customer_id, customer_name, segment)
            VALUES (%s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """, (row['customer_id'], row['customer_name'], row['segment']))


def load_dim_product(cursor, df):
    products = df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates()
    for _, row in products.iterrows():
        cursor.execute("""
            INSERT INTO dim_product (product_id, product_name, category, sub_category)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING;
        """, (row['product_id'], row['product_name'], row['category'], row['sub_category']))


def load_dim_location(cursor, df):
    locations = df[['country', 'region', 'state', 'city', 'postal_code']].drop_duplicates()
    location_map = {}

    for _, row in locations.iterrows():
        cursor.execute("""
            INSERT INTO dim_location (country, region, state, city, postal_code)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (country, region, state, city, postal_code) DO NOTHING
            RETURNING location_id;
        """, (row['country'], row['region'], row['state'], row['city'], row['postal_code']))

        result = cursor.fetchone()
        if result:
            location_id = result[0]
        else:
            # If already exists, fetch the id
            cursor.execute("""
                SELECT location_id FROM dim_location
                WHERE country=%s AND region=%s AND state=%s AND city=%s AND postal_code=%s;
            """, (row['country'], row['region'], row['state'], row['city'], row['postal_code']))
            location_id = cursor.fetchone()[0]

        location_map[(
            row['country'], row['region'], row['state'], row['city'], row['postal_code']
        )] = location_id

    return location_map


def load_dim_date(cursor, df):
    dates = df[['order_date']].drop_duplicates()
    for _, row in dates.iterrows():
        date = row['order_date']
        cursor.execute("""
            INSERT INTO dim_date (date_id, year, quarter, month, month_name, day, day_of_week)
            VALUES (%s, EXTRACT(YEAR FROM %s), EXTRACT(QUARTER FROM %s),
                    EXTRACT(MONTH FROM %s), TO_CHAR(%s, 'Month'),
                    EXTRACT(DAY FROM %s), TO_CHAR(%s, 'Day'))
            ON CONFLICT (date_id) DO NOTHING;
        """, (date, date, date, date, date, date, date))


def load_fact_sales(cursor, df, location_map):
    for _, row in df.iterrows():
        location_key = (
            row['country'], row['region'], row['state'], row['city'], row['postal_code']
        )

        cursor.execute("""
            INSERT INTO fact_sales (
                order_id, customer_id, product_id, location_id,
                order_date, ship_date, sales, quantity, discount, profit
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (
            row['order_id'], row['customer_id'], row['product_id'],
            location_map[location_key],
            row['order_date'], row['ship_date'],
            row['sales'], row['quantity'], row['discount'], row['profit']
        ))


def load_data():
    df = transform_data()
    print(f"Total rows in transformed df: {len(df)}")  # Debug print
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        load_dim_customer(cursor, df)
        load_dim_product(cursor, df)
        location_map = load_dim_location(cursor, df)
        load_dim_date(cursor, df)
        load_fact_sales(cursor, df, location_map)
        conn.commit()
        print("Data loaded successfully!")
    except Exception as e:
        conn.rollback()
        print("Error loading data:", e)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_data()
