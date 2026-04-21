from pathlib import Path
from uuid import UUID



class InvalidCSVError(Exception):
    """Raised when a file fails CSV validation —
    either by extension check or content sniffing."""
    
    def __init__(self, path: Path|str) -> None:
        self.path = path
        super().__init__(f"File must have csv format: {path}")

class InvoiceNotFoundError(Exception):
    """Raised when the provided invoice_id is not found in database."""

    def __init__(self, invoice_id: UUID) -> None:
        self.invoice_id = invoice_id
        super().__init__(f"Invoice with id {invoice_id} not found in database")