from pydantic import BaseModel

from schemas.invoice import Invoice, InvoiceLineItem
from schemas.columns_mapping import ColumnMapping



class RawInvoice(BaseModel):
    model_config = {"extra": "allow"}
    

class IngestionResult(BaseModel):
    """In-memory class to store result from normalization / ingestion phase."""
    invoice: Invoice
    invoice_line_items: list[InvoiceLineItem]
    column_mapping_results: list[ColumnMapping]