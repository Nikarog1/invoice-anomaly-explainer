from datetime import date
from uuid import UUID, uuid4

from enum import Enum
from pydantic import BaseModel
from sqlmodel import Field, SQLModel 



class Contract(SQLModel, table=True):
    contract_id: UUID = Field(default_factory=uuid4, primary_key=True)
    supplier_name: str
    buyer_name: str
    currency: str | None = None
    payment_terms_days: int | None = None
    payment_details: str | None = None
    signed_on: date
    expires_on: date | None = None
    notes: str | None = None
    
class ContractLineItem(SQLModel, table=True):
    contract_line_item_id: UUID = Field(default_factory=uuid4, primary_key=True)
    contract_id: UUID = Field(foreign_key="contract.contract_id")
    product_service_name: str
    product_service_description: str | None = None
    unit_price: float
    max_units: float | None = None
    units_kind: str | None = None
    notes: str | None = None
    
# Accumulator for pipeline state
class ContractWithLineItems(BaseModel): 
    contract: Contract
    line_items: list[ContractLineItem]

class DegradationReason(str, Enum):
    no_contract = "no_contract"
    issue_date_missing = "issue_date_missing"
       
class ContractSummary(BaseModel):
    contracts: list[ContractWithLineItems]
    is_degraded: bool
    degradation_reason: DegradationReason | None