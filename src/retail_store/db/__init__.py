from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

raw_data_engine = create_engine("sqlite:///src/retail_store/db/raw_data.db", echo=True)
staging_engine = create_engine("sqlite:///src/retail_store/db/staging.db", echo=True)
public_engine = create_engine("sqlite:///src/retail_store/db/public.db", echo=True)