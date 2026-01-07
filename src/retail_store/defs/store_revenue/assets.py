import dagster as dg
import pandas as pd
from retail_store.db import raw_data_engine
from pathlib import Path

@dg.asset(group_name="Extract")
def extract_retail_transactions(context: dg.AssetExecutionContext):
    data_path = Path("src/retail_store/data/Retail_Transaction_Dataset.csv")
    try:
        context.log.info("Attempting to extract retail transactions from {data_path}")
        df = pd.read_csv(data_path)
    except:
        raise dg.Failure(f"Failed to extract retail transactions")
    context.log.info(f"Extracted {len(df)} rows from {data_path}")
    return df

@dg.asset(group_name="Load", deps=[extract_retail_transactions])
def load_retail_transactions(context: dg.AssetExecutionContext, extract_retail_transactions: pd.DataFrame):
    df: pd.DataFrame = extract_retail_transactions
    try:
        context.log.info(f"Attempting to load {len(df)} rows to retail_transactions")
        df.to_sql("retail_transactions", raw_data_engine, if_exists="append", index=False)
    except:
        raise dg.Failure(f"Failed to load retail transactions")
    raw_data_retail_transactions = pd.read_sql("SELECT * FROM retail_transactions", raw_data_engine)
    return raw_data_retail_transactions

@dg.asset(group_name="Staging", deps=[load_retail_transactions])
def stage_retail_transactions(context: dg.AssetExecutionContext, load_retail_transactions: pd.DataFrame):
    raw_data_retail_transactions = load_retail_transactions
    


