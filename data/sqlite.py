from sqlmodel import Session, SQLModel, create_engine

from config.settings import settings

from schemas.anomaly import AnomalyFlag, AnomalyReport
from schemas.columns_mapping import ColumnMapping
from schemas.contract import Contract, ContractLineItem
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.junction import LineItemMatch



engine = create_engine(settings.sqlite_url, echo=True) # in prod change to echo=False; potentially add async option connect_args={"check_same_thread": False}

def create_db_and_tables():  
    SQLModel.metadata.create_all(engine)
    
def get_session():
    return Session(engine)