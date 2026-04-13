from typing import Literal
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel 



class LineItemMatch(SQLModel, table=True):
    line_item_match_id: UUID = Field(default_factory=uuid4, primary_key=True)
    contract_line_item_id: UUID = Field(foreign_key="contractlineitem.contract_line_item_id")
    invoice_line_item_id: UUID = Field(foreign_key="invoicelineitem.invoice_line_item_id")
    match_method: Literal["exact", "fuzzy", "vector", "llm"]
    match_score: float