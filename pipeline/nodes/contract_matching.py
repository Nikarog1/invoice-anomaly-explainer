from rapidfuzz import fuzz

from sqlmodel import or_, select

from config.settings import settings

from core.exceptions import PipelineStateError
from core.logging import get_logger

from data.sqlite import get_session

from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag
from schemas.contract import ContractLineItem
from schemas.invoice import InvoiceLineItem
from schemas.junction import LineItemMatch, Method

logger = get_logger(__name__)



def contract_matching(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    logger.info("Running contract_matching")
    
    invoice = state["invoice"]
    invoice_line_items = state["invoice_line_items"]
    contract_summary = state["contract_summary"]
    
    if (
        invoice is None
        or invoice_line_items is None or 
        contract_summary is None
    ):
        raise PipelineStateError("invoice_line_items and/or contract_summary")
    
    if contract_summary.is_degraded: # additional check, this one should be checked by conditional edge
        return {"anomaly_flags": []}
    
    # below checks if some of invoice_line_item_id is already mapped
    with get_session() as session:
        line_item_match_already_mapped = session.exec(
            select(LineItemMatch)
            .where(LineItemMatch.invoice_line_item_id.in_([item.invoice_line_item_id for item in invoice_line_items])) # type: ignore
        ).all()

    if len(line_item_match_already_mapped) > 0:
        logger.info(f"{len(line_item_match_already_mapped)} items already mapped to contract")
         
    # unfilter already mapped items
    line_item_id_already_mapped = [item.invoice_line_item_id for item in line_item_match_already_mapped]
    invoice_line_items_filtered = [
        item 
        for item in invoice_line_items 
        if item.invoice_line_item_id not in line_item_id_already_mapped
    ]
    
    contract_candidates: list[ContractLineItem] = [
        item
        for contract in contract_summary.contracts
        for item in contract.line_items
    ]
    
    results = []
    for inv_line in invoice_line_items_filtered:
        matched = _exact_match(inv_line, contract_candidates)
        
        if matched:
            results.append(
                LineItemMatch(
                    contract_line_item_id=matched.contract_line_item_id, 
                    invoice_line_item_id=inv_line.invoice_line_item_id,
                    match_method=Method.exact,
                    match_score=1.0
                )
            )
            continue
        
        matched_fuzzy, score = _fuzzy_match(inv_line, contract_candidates, settings.thresholds.pipeline_fuzzy_match_min)
        
        if matched_fuzzy:
            results.append(
                LineItemMatch(
                    contract_line_item_id=matched_fuzzy.contract_line_item_id, 
                    invoice_line_item_id=inv_line.invoice_line_item_id,
                    match_method=Method.fuzzy,
                    match_score=score
                )
            )
            continue
        
        matched_vector, score = _vector_match(
            inv_line, 
            contract_candidates,
            invoice.supplier_name,
            settings.thresholds.pipeline_vector_match_min,
        )
        
        if matched_vector:
            results.append(
                LineItemMatch(
                    contract_line_item_id=matched_vector.contract_line_item_id, 
                    invoice_line_item_id=inv_line.invoice_line_item_id,
                    match_method=Method.vector,
                    match_score=score
                )
            )
            continue
            

def _exact_match(invoice_line_item: InvoiceLineItem, contract_line_items: list[ContractLineItem]) -> ContractLineItem | None:
    """
    Perform exact match of contract product / service name on invoice line item description.
    """
    contract = next(
        (
            contract_item 
            for contract_item in contract_line_items
            if contract_item.product_service_name == invoice_line_item.description
        ),
        None
    )
    return contract


def _fuzzy_match(
        invoice_line_item: InvoiceLineItem, 
        contract_line_items: list[ContractLineItem],
        confidence_threshold: float = settings.thresholds.pipeline_fuzzy_match_min,
        
    ) -> tuple[ContractLineItem | None, float]:
    """
    Perform fuzzy match of contract product / service name on invoice line item description.
    Search for best confidence score iterating over all contract line items.
    If several contract names have same score, first one occured is returned.
    """
    best_score = 0.0
    best_contract_item = None
    
    for contract_item in contract_line_items:
        score = fuzz.token_set_ratio(contract_item.product_service_name, invoice_line_item.description) / 100
        
        if score > best_score:
            best_score = score
            best_contract_item = contract_item
            
    if best_score >= confidence_threshold:    
        return (best_contract_item, best_score)
    
    else:
        return (None, 0.0)


def _vector_match(
        invoice_line_item: InvoiceLineItem, 
        contract_line_items: list[ContractLineItem],
        supplier_name: str,
        confidence_threshold: float = settings.thresholds.pipeline_vector_match_min,
    ) -> tuple[ContractLineItem | None, float]:
    """
    Perform vector match of contract product / service name on invoice line item description.
    """
    contract_ids = [str(c.contract_line_item_id) for c in contract_line_items]
    
    result = collection.query(
        query_texts=[invoice_line_item.description],
        n_results=1,
        where={
            "supplier_name": supplier_name,
            "contract_line_item_id": {"$in": contract_ids}
        },
    )
    
    if not result["ids"][0]:
        return (None, 0.0)
    
    matched_id = result["ids"][0][0]
    distance = result["distances"][0][0]
    similarity = 1 - distance
    
    if similarity < confidence_threshold:
        return (None, 0.0)
    
    matched_item = next(c for c in contract_line_items if str(c.contract_line_item_id) == matched_id)
    return (matched_item, similarity)