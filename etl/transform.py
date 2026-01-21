"""
transform.py

Purpose:
    Clean and transform raw Superstore sales data for loading into a data warehouse.

Author: Sindhu Srinivas
"""

import pandas as pd
from extract import extract_sales_data  # make sure extract.py is in the same folder


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names:
    - strip spaces
    - lowercase
    - replace spaces/dashes with underscores
    - rename to match load.py expectations
    """
    # lowercase, strip spaces, replace dash/space with underscore
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

    # map CSV columns to expected names
    rename_map = {
        'customer_id': 'customer_id',
        'customername': 'customer_name',   # in case the CSV column is "CustomerName"
        'customer_name': 'customer_name',
        'segment': 'segment',
        'product_id': 'product_id',
        'productname': 'product_name',
        'product_name': 'product_name',
        'category': 'category',
        'sub_category': 'sub_category',
        'order_id': 'order_id',
        'orderdate': 'order_date',
        'order_date': 'order_date',
        'shipdate': 'ship_date',
        'ship_date': 'ship_date',
        'sales': 'sales',
        'quantity': 'quantity',
        'discount': 'discount',
        'profit': 'profit',
        'country': 'country',
        'region': 'region',
        'state': 'state',
        'city': 'city',
        'postal_code': 'postal_code'
    }

    # Only rename columns that exist in df
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    return df


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to raw sales data.
    """
    df = clean_column_names(df)

    # Convert date columns
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["ship_date"] = pd.to_datetime(df["ship_date"])

    # Drop columns not needed
    if "row_id" in df.columns:
        df = df.drop(columns=["row_id"])

    # Remove rows with missing critical values
    df = df.dropna(subset=["order_id", "sales", "profit", "customer_id", "product_id"])

    # Ensure numeric columns are correct
    numeric_columns = ["sales", "profit", "quantity", "discount"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with numeric conversion issues
    df = df.dropna(subset=numeric_columns)

    return df


def transform_data() -> pd.DataFrame:
    """
    Wrapper function for load.py:
    Extract + transform + return clean DataFrame
    """
    raw_file_path = "data/raw/superstore.csv"
    raw_df = extract_sales_data(raw_file_path)
    return transform_sales_data(raw_df)


if __name__ == "__main__":
    df = transform_data()
    print("Columns after transformation:")
    print(df.columns.tolist()) 
    print("Preview of data:")
    print(df.head())
    print(f"Total rows after transformation: {len(df)}")

