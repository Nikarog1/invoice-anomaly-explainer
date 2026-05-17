from sqlmodel import Session, SQLModel, create_engine

from config.settings import settings

from schemas.anomaly import AnomalyFlag, AnomalyReport
from schemas.columns_mapping import ColumnMapping
from schemas.contract import Contract, ContractLineItem
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.jobs import AnalysisJob, IngestionJob
from schemas.junction import LineItemMatch



engine = create_engine(settings.sqlite_url, echo=False)

def create_db_and_tables() -> None:  
    SQLModel.metadata.create_all(engine)
    
def get_session() -> Session:
    return Session(engine, expire_on_commit=False)