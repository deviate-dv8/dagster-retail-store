from sqlalchemy import create_engine

# Single warehouse database (SQLite doesn't support schemas, so we use table name prefixes)
# raw_data_* tables = raw_data schema
# staging_* tables = staging schema  
# dim_* and fact_* tables = public/transform schema
engine = create_engine("sqlite:///src/retail_store/db/public.db", echo=True)

# For backward compatibility, keep these aliases pointing to the same engine
raw_data_engine = engine
staging_engine = engine
public_engine = engine