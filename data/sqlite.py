from collections.abc import Generator
from uuid import UUID

from sqlmodel import create_engine, desc, select, Session, SQLModel

from config.settings import settings
from core.exceptions import JobNotFoundError, InvoiceNotFoundError

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

def load_ingestion_job(session: Session, job_id: UUID) -> IngestionJob:
    ingestion_job = session.get(IngestionJob, job_id)
    if ingestion_job is None:
        raise JobNotFoundError(job_id)
        
    return ingestion_job

def load_analysis_job(session: Session, job_id: UUID) -> AnalysisJob:
    analysis_job = session.get(AnalysisJob, job_id)
    if analysis_job is None:
        raise JobNotFoundError(job_id)
        
    return analysis_job

def invoice_exists(session: Session, invoice_id: UUID) -> bool:
    if session.get(Invoice, invoice_id):
        return True
    return False
    
def load_latest_analysis_job(session: Session, invoice_id: UUID, status: str | None) -> AnalysisJob | None:
    query = (
        select(AnalysisJob)
        .where(AnalysisJob.invoice_id == invoice_id)
        .order_by(desc(AnalysisJob.created_at))
    )
    if status:
        query = query.where(AnalysisJob.status == status)

    return session.exec(query).first()