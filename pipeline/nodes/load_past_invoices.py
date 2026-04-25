from dateutil.relativedelta import relativedelta
from sqlmodel import col, select
from statistics import stdev

from core.exceptions import PipelineStateError, InvoiceValueNotFoundError
from core.logging import get_logger
from config.settings import settings

from data.sqlite import get_session
from pipeline.state import PipelineState

from schemas.history import DegradationReason, HistoricalSummary, LineItemStats
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.supplier_config import SupplierConfig

logger = get_logger(__name__)



def load_past_invoices(state: PipelineState) -> dict:
    """
    Load historical invoices for supplier on analyzed invoice and compute HistoricalSummary.

    Uses defaults from settings (window = N months back from invoice.issue_date, min_samples)
    unless SupplierConfig row overrides them for this supplier.

    Returned HistoricalSummary states:
        - is_degraded=False: enough in-window samples; stats computed from window.
        - is_degraded=True, no_history: supplier has no past invoices.
        - is_degraded=True, window_miss: not enough in-window, but enough in full history.
        - is_degraded=True, thin_count: not enough samples in full history either.

    In degraded cases (window_miss, thin_count), stats are computed from full history
    rather than window — caveat is surfaced via degradation_reason field.

    Raises:
        PipelineStateError: if invoice or invoice_line_items missing from state.
        InvoiceValueNotFoundError: if invoice.issue_date is None and no SupplierConfig override exists.
    """
    
    logger.info("Running load_past_invoices")
    
    invoice: Invoice | None = state["invoice"]
    invoice_line_items: list[InvoiceLineItem] | None = state["invoice_line_items"]
    
    if invoice is None or invoice_line_items is None:
        raise PipelineStateError("invoice or/and invoice_line_items") 
    
    invoice_id = state["invoice_id"]
    supplier_name = invoice.supplier_name
    buyer_name = invoice.buyer_name
    issue_date = invoice.issue_date
    
    with get_session() as session:

        custom_config = session.exec(
            select(SupplierConfig)
            .where(SupplierConfig.supplier_name == supplier_name)
        ).first()
       
        if custom_config:
            logger.debug("Custom supplier config identified")
            cutoff_date = custom_config.min_history_date
            min_samples = custom_config.min_samples if custom_config.min_samples is not None else settings.suppliers_config.default_min_samples
            
        else:
            
            if issue_date is None:
                    raise InvoiceValueNotFoundError("issue_date")
            
            logger.debug("No custom supplier config identified, using default thresholds")
            cutoff_date = issue_date - relativedelta(months=settings.suppliers_config.default_history_window_months)
            min_samples = settings.suppliers_config.default_min_samples
               
        historical_invoices = session.exec(
            select(Invoice)
            .where(Invoice.invoice_id != invoice_id)
            .where(Invoice.supplier_name == supplier_name)
            .where(Invoice.buyer_name == buyer_name)
        ).all()
        logger.debug(f"Found {len(historical_invoices)} historical invoices for {supplier_name}")
        
        
        if len(historical_invoices) > 0:
        
            historical_invoices_window = [
                hist_inv for hist_inv 
                in historical_invoices 
                if hist_inv.issue_date is not None 
                and hist_inv.issue_date >= cutoff_date
            ]
            
            if len(historical_invoices_window) >= min_samples:
                historical_invoices = historical_invoices_window
                is_degraded = False
                degradation_reason = None
            
            else:
                if len(historical_invoices) >= min_samples:
                    is_degraded = True
                    degradation_reason = DegradationReason.window_miss
                else:
                    is_degraded = True
                    degradation_reason = DegradationReason.thin_count
                    
        else:
            is_degraded = True
            degradation_reason = DegradationReason.no_history    
              
        historical_invoices_ids = [hist_inv.invoice_id for hist_inv in historical_invoices]
        historical_invoices_items = list(
            session.exec(
                select(InvoiceLineItem)
                .where(col(InvoiceLineItem.invoice_id).in_(historical_invoices_ids))
            ).all()
        )
             
    fields_seen = set()
    line_stats: dict[str, list[float]] = {}
    
    for line_item in historical_invoices_items:
        descr = line_item.description
        amount = line_item.amount_gross
        
        fields_seen.update(line_item.model_dump().keys())
        
        if descr in line_stats:
            line_stats[descr].append(amount)
            
        else:
            line_stats[descr] = [amount]
     
    results_line_item = []       
    for line_item, amounts in line_stats.items():
        results_line_item.append(
            LineItemStats(
                description=line_item,
                mean_amount=sum(amounts) / len(amounts),
                stddev_amount=stdev(amounts) if len(amounts) > 1 else None,
                n_samples=len(amounts)
            )
        )
    
    fields_seen.update(
        key
        for hist_inv in historical_invoices
        for key in hist_inv.model_dump().keys()
    )
    
    metadata_keys_seen = set(
        key
        for hist_inv in historical_invoices
        for key in hist_inv.invoice_metadata.keys()
    )

    historical_summary = HistoricalSummary(
        supplier_name=supplier_name,
        invoice_count=len(historical_invoices),
        fields_seen=fields_seen,
        metadata_keys_seen=metadata_keys_seen,
        line_item_stats=results_line_item,
        is_degraded=is_degraded,
        degradation_reason=degradation_reason,
    )
    logger.info(
        f"Historical summary: supplier={supplier_name}, count={len(historical_invoices)}, "
        f"degraded={is_degraded}, reason={degradation_reason}"
    )
    return {
        "historical_summary": historical_summary
    }
            

            

    
