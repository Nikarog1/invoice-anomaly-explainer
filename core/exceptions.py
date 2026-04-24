from pathlib import Path
from uuid import UUID



class InvalidCSVError(Exception):
    """Raised when file fails CSV validation —
    either by extension check or content sniffing."""
    
    def __init__(self, path: Path|str) -> None:
        self.path = path
        super().__init__(f"File must have csv format: {path}.")

class InvoiceNotFoundError(Exception):
    """Raised when provided invoice_id is not found in database."""

    def __init__(self, invoice_id: UUID) -> None:
        self.invoice_id = invoice_id
        super().__init__(f"Invoice with id {invoice_id} not found in database.")
        
class InvoiceValueNotFoundError(Exception):
    """Raised when provided invoice has field required for further analysis equal None."""

    def __init__(self, field: str) -> None:
        self.field = field
        super().__init__(f"Invoice field {self.field} is None")
        
class InvoiceMappingNotFoundError(Exception):
    """Raised when column mapping for invoice_id is not found in database."""

    def __init__(self, invoice_id: UUID) -> None:
        self.invoice_id = invoice_id
        super().__init__(f"Mapping for invoice id {invoice_id} not found in database.")
        
class IngestionRepositoryError(Exception):
    """Raised when encountering error during save / load to db in ingestion phase"""

    def __init__(self, invoice_id: UUID) -> None:
        self.invoice_id = invoice_id
        super().__init__(f"Failed to proceed invoice id {invoice_id}.")
        
class PipelineStateError(Exception):
    """Raised when pipeline state misses required input from previous node(s)."""
    
    def __init__(self, field_name: str):
        self.field_name = field_name
        super().__init__(f"Required state field '{field_name}' is None.")
    