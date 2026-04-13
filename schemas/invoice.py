from datetime import date
from uuid import UUID, uuid4

from sqlmodel import Column, Field, JSON, SQLModel 



class Invoice(SQLModel, table=True):
    invoice_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_number: str
    supplier_name: str
    buyer_name: str
    issue_date: date
    due_date: date
    total_amount: float
    currency: str
    payment_details: str
    invoice_metadata: dict = Field(default={}, sa_column=Column(JSON))  # country-specific fields like dic in CZ
    
class InvoiceLineItem(SQLModel, table=True):
    invoice_line_item_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    description: str
    quantity: float
    unit_price: float | None = None
    amount_net: float
    amount_gross: float
    vat_rate: float
    notes: str | None = None