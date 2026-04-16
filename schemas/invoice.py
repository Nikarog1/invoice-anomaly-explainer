from datetime import date
from uuid import UUID, uuid4

from sqlmodel import Column, Field, JSON, SQLModel 



class Invoice(SQLModel, table=True):
    invoice_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_number: str
    supplier_name: str
    buyer_name: str | None = None
    issue_date: date | None = None
    due_date: date | None = None
    total_amount: float
    currency: str | None = None
    payment_details: str | None = None
    invoice_metadata: dict = Field(default={}, sa_column=Column(JSON))  # country-specific fields like dic in CZ
    
class InvoiceLineItem(SQLModel, table=True):
    invoice_line_item_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    description: str
    quantity: float | None = None
    unit_price: float | None = None
    amount_net: float | None = None
    amount_gross: float
    vat_rate: float | None = None
    notes: str | None = None