"""
transform.py

Purpose:
    Clean and transform raw Superstore sales data for loading into a data warehouse.

Author: Sindhu Srinivas
"""

import pandas as pd


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names:
    - lowercase
    - replace spaces with underscores
    """
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to raw sales data.
    """
    df = clean_column_names(df)

    # Convert date columns
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["ship_date"] = pd.to_datetime(df["ship_date"])

    # Drop columns that are not useful for analytics
    if "row_id" in df.columns:
        df = df.drop(columns=["row_id"])

    # Remove rows with missing critical values
    df = df.dropna(subset=["order_id", "sales", "profit"])

    # Ensure numeric columns are correct
    numeric_columns = ["sales", "profit", "quantity", "discount"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Final cleanup: drop rows where numeric conversion failed
    df = df.dropna(subset=numeric_columns)

    return df


if __name__ == "__main__":
    # Local testing
    from extract import extract_sales_data

    raw_file_path = "data/raw/superstore.csv"
    raw_df = extract_sales_data(raw_file_path)

    transformed_df = transform_sales_data(raw_df)

    print("Transformed data preview:")
    print(transformed_df.head())
    print(f"Total rows after transformation: {len(transformed_df)}")
