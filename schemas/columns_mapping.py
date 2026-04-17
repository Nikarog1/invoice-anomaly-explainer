from __future__ import annotations

from pydantic import BaseModel, model_validator
from typing import Literal, Self



class ColumnMappingResult(BaseModel):
    raw_column: str
    schema_field: str | None
    method: Literal["exact", "fuzzy", "llm"]
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
        
    