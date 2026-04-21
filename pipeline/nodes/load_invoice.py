from sqlmodel import select

from core.exceptions import InvoiceNotFoundError
from core.logging import get_logger

from data.sqlite import get_session

from pipeline.state import PipelineState

from schemas.invoice import Invoice, InvoiceLineItem

logger = get_logger(__name__)



def load_invoice(state: PipelineState) -> dict:
    logger.info("Running load_invoice")
    
    invoice_id = state["invoice_id"]
    
    with get_session() as session:
        invoice = session.get(Invoice, invoice_id) # not SQLModel method but SQLAlchemy (inheretation)
        if invoice is None:
            raise InvoiceNotFoundError(invoice_id)
        invoice_line_items = session.exec(
            select(InvoiceLineItem).where(InvoiceLineItem.invoice_id == invoice_id)
        ).all() # SQLAlchemy ScalarResult.all() returns list
        
    logger.info(f"Loaded invoice {invoice_id} with {len(invoice_line_items)} line items") 
       
    return {
        "invoice": invoice,
        "invoice_line_items": invoice_line_items
    }
        
        