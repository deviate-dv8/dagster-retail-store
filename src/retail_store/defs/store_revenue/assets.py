import dagster as dg
import pandas as pd
from retail_store.db import raw_data_engine
from pathlib import Path

@dg.asset(group_name="Extract")
def extract_retail_transactions(context: dg.AssetExecutionContext):
    data_path = Path("src/retail_store/data/Retail_Transaction_Dataset.csv")
    try:
        context.log.info(f"Attempting to extract retail transactions from {data_path}")
        df = pd.read_csv(data_path)
        context.log.info(f"Extracted {len(df)} rows from {data_path}")
    except Exception as e:
        raise dg.Failure(f"Failed to extract retail transactions: {e}")
    return df

@dg.asset(group_name="Load", deps=[extract_retail_transactions])
def load_retail_transactions(context: dg.AssetExecutionContext, extract_retail_transactions: pd.DataFrame):
    df: pd.DataFrame = extract_retail_transactions
    try:
        # Use table name prefix: raw_data_retail_transactions (simulates raw_data.retail_transactions schema)
        context.log.info(f"Attempting to load {len(df)} rows to raw_data_retail_transactions")
        df.to_sql("raw_data_retail_transactions", raw_data_engine, if_exists="append", index=False)
        context.log.info(f"Loaded {len(df)} rows to raw_data_retail_transactions")
    except Exception as e:
        raise dg.Failure(f"Failed to load retail transactions: {e}")
    raw_data_retail_transactions = pd.read_sql("SELECT * FROM raw_data_retail_transactions", raw_data_engine)
    return raw_data_retail_transactions

@dg.asset(group_name="Staging", deps=[load_retail_transactions])
def stage_retail_transactions(context: dg.AssetExecutionContext, load_retail_transactions: pd.DataFrame):
    raw_data_retail_transactions = load_retail_transactions
    


