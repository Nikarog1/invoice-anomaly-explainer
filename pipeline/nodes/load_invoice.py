from core.logging import get_logger
from data.sqlite import get_session, load_invoice_from_sql
from pipeline.state import PipelineState

logger = get_logger(__name__)



def load_invoice(state: PipelineState) -> dict:
    logger.info("Running load_invoice")
    
    invoice_id = state["invoice_id"]
    
    with get_session() as session:
        invoice, invoice_line_items = load_invoice_from_sql(session, invoice_id)
        
    logger.info(f"Loaded invoice {invoice_id} with {len(invoice_line_items)} line items") 
       
    return {
        "invoice": invoice,
        "invoice_line_items": invoice_line_items
    }
        
        