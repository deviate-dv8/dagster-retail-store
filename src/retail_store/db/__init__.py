from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

engine = create_engine("sqlite:///src/retail_store/db/warehouse.db", echo=True)

Session = sessionmaker(bind=engine)
