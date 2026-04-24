from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import select

from core.exceptions import IngestionRepositoryError, InvoiceMappingNotFoundError
from core.logging import get_logger

from data.sqlite import get_session
from ingestion.models import IngestionResult

from schemas.columns_mapping import ColumnMappingResult
from schemas.invoice import Invoice, InvoiceLineItem

logger = get_logger(__name__)



class IngestionRepository:
    """Stores and loads objects from ingestion phase"""
    def save(self, ingestion_result: IngestionResult) -> None:
        invoice: Invoice = ingestion_result.invoice
        invoice_id: UUID = invoice.invoice_id
        invoice_line_items: list[InvoiceLineItem] = ingestion_result.invoice_line_items
        
        column_mapping_results = [
            ColumnMappingResult(invoice_id=invoice_id, **cm.model_dump())
            for cm in ingestion_result.column_mapping_results
        ]
            
        with get_session() as session:
            try:
                session.add(invoice)
                session.add_all(invoice_line_items)
                session.add_all(column_mapping_results)
                session.commit()
            except SQLAlchemyError as e:
                raise IngestionRepositoryError(invoice_id) from e
            
        logger.info(f"Saved invoice {invoice_id}: {len(invoice_line_items)} line items, {len(column_mapping_results)} mappings")
            
    def load_mappings(self, invoice_id: UUID) -> list[ColumnMappingResult]:
        with get_session() as session:
            try:
                column_mapping_results = list(
                    session.exec(
                        select(ColumnMappingResult)
                        .where(ColumnMappingResult.invoice_id == invoice_id)
                    ).all()
                )
            except SQLAlchemyError as e:
                raise IngestionRepositoryError(invoice_id) from e
            
        if not column_mapping_results:
            raise InvoiceMappingNotFoundError(invoice_id)
            
        return column_mapping_results
                
        
