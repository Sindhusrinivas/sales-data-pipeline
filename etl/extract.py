

"""
extract.py

Purpose:
    Read raw Superstore CSV data from disk and load it into a Pandas DataFrame.

Author: Sindhu Srinivas
"""

import os
import pandas as pd


def extract_sales_data(file_path: str) -> pd.DataFrame:
    """
    Extract sales data from a CSV file.

    Args:
        file_path (str): Path to the raw CSV file

    Returns:
        pd.DataFrame: Raw sales data
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at path: {file_path}")

    df = pd.read_csv(file_path)

    return df


if __name__ == "__main__":
    # Local testing
    raw_file_path = "data/raw/superstore.csv"
    df = extract_sales_data(raw_file_path)

    print("Extracted data preview:")
    print(df.head())
    print(f"Total rows extracted: {len(df)}")
