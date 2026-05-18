from collections.abc import Generator
from uuid import UUID

from sqlmodel import select, Session, SQLModel, create_engine

from config.settings import settings
from core.exceptions import InvoiceNotFoundError

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

def load_invoice_from_sql(session: Session, invoice_id: UUID) -> tuple[Invoice, list[InvoiceLineItem]]:
    invoice = session.get(Invoice, invoice_id)
    if invoice is None:
        raise InvoiceNotFoundError(invoice_id)
    invoice_line_items = session.exec(
        select(InvoiceLineItem).where(InvoiceLineItem.invoice_id == invoice_id)
    ).all()
        
    return invoice, list(invoice_line_items)
        