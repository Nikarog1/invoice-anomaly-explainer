from datetime import date, datetime, timezone
from uuid import UUID

from sqlmodel import Field, SQLModel



class SupplierConfig(SQLModel, table=True):
    """Stores configurations for individual suppliers for historical invoices extraction."""
    supplier_name: str  = Field(primary_key=True)
    min_history_date: date
    min_samples: int | None  
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc)) # New datetime inserted once supplir has updated configs
    user_id: UUID | None = None