from __future__ import annotations

from typing import Self
from uuid import uuid4, UUID

from enum import Enum
from pydantic import BaseModel, model_validator
from sqlmodel import Field, SQLModel 



class MappingMethod(str, Enum):
    exact = "exact"
    fuzzy = "fuzzy"
    llm = "llm"
  
class ColumnMappingResult(BaseModel):
    """In-memory class to store mapping results during normalization."""
    raw_column: str
    schema_field: str | None
    method: MappingMethod
    resolved: bool
    confidence: float | None
    
    @model_validator(mode="after")
    def validate_fields(self) -> Self:
        if (
            self.schema_field is None and self.resolved
        )\
        or (
            self.schema_field is not None and not self.resolved
        ):
            raise ValueError(f"Value conflict between schema_field {self.schema_field} and resolved {self.resolved}")
        return self


class ColumnMapping(SQLModel, table=True):
    """SQLModel to store mapping results after normalization during ingestion of Invoice and InvoiceLineItems."""
    column_mapping_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    raw_column: str
    schema_field: str | None
    method: MappingMethod  
    resolved: bool
    confidence: float | None
    
    @model_validator(mode="after")
    def validate_fields(self) -> Self:
        if (
            self.schema_field is None and self.resolved
        )\
        or (
            self.schema_field is not None and not self.resolved
        ):
            raise ValueError(f"Value conflict between schema_field {self.schema_field} and resolved {self.resolved}")
        return self
    