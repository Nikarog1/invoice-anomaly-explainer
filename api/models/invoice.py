from datetime import date
from uuid import UUID

from pydantic import BaseModel



class InvoiceLineItemDTO(BaseModel):
    description: str
    quantity: float | None = None
    unit_price: float | None = None
    amount_net: float | None = None
    amount_gross: float
    vat_rate: float | None = None
    notes: str | None = None
    
class InvoiceDTO(BaseModel):
    invoice_id: UUID
    invoice_number: str
    supplier_name: str
    buyer_name: str | None
    issue_date: date | None
    due_date: date | None
    total_amount: float
    currency: str | None
    payment_details: str | None
    metadata: dict  # country-specific fields (DIC, VAT keys, etc.)
    line_items: list[InvoiceLineItemDTO]