from uuid import UUID

from sqlmodel import or_, select

from core.exceptions import PipelineStateError
from core.logging import get_logger

from data.sqlite import get_session

from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag
from schemas.contract import Contract, ContractLineItem, ContractSummary, ContractWithLineItems, DegradationReason
from schemas.junction import LineItemMatch

logger = get_logger(__name__)



def contract_matching(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    logger.info("Running contract_matching")
    
    invoice_line_items = state["invoice_line_items"]
    contract_summary = state["contract_summary"]
    
    if invoice_line_items is None or contract_summary is None:
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
    invoice_line_items_filtered = [item for item in invoice_line_items if item.invoice_line_item_id not in line_item_id_already_mapped]
    
    # re transform contract lines - RETHINK LATER
    contract_items_name_desc: dict[UUID, tuple] = dict()
    for contract in contract_summary.contracts:
        for item in contract.line_items:
            contract_items_name_desc[item.contract_line_item_id] = (item.product_service_name, item.product_service_description)

    # re transform invoice lines - RETHINK LATER      
    invoice_items_name_desc: dict[UUID, str] = dict()
    for invoice_item in invoice_line_items_filtered:
        invoice_items_name_desc[invoice_item.invoice_line_item_id] = invoice_item.description
        
    # exact match - RETHINK LATER
    results = []
    for i_line_id, i_line_desc in invoice_items_name_desc.items():
        if i_line_desc in contract_items_name_desc.values(): # this one is wrong, comparing individual desc (str) to tuple
            results.append(
                LineItemMatch(
                )
            )
            
            
        

    
    
        

        