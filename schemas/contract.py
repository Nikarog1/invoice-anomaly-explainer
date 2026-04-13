from datetime import date
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel 



class Contract(SQLModel, table=True):
    contract_id: UUID = Field(default_factory=uuid4, primary_key=True)
    supplier_name: str
    buyer_name: str
    currency: str
    payment_terms_days: int
    payment_details: str
    signed_on: date
    expires_on: date | None = None
    notes: str | None = None
    
class ContractLineItem(SQLModel, table=True):
    contract_line_item_id: UUID = Field(default_factory=uuid4, primary_key=True)
    contract_id: UUID = Field(foreign_key="contract.contract_id")
    product_service_name: str
    product_service_description: str | None = None
    unit_price: float
    max_units: float
    units_kind: str
    notes: str | None = None