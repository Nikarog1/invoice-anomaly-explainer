from uuid import UUID

from rapidfuzz import fuzz

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import select

from config.prompts import CONTRACT_MATCHING_PROMPT
from config.settings import settings

from core.exceptions import PipelineRepositoryError, PipelineStateError
from core.llm_client import call_local_llm
from core.logging import get_logger

from data.sqlite import get_session
from data.vector_store import get_collection, query_similar

from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.contract import ContractLineItem
from schemas.invoice import InvoiceLineItem
from schemas.junction import LineItemMatch, Method

logger = get_logger(__name__)



async def contract_matching(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
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
    
    if contract_summary.is_degraded:
        return {"anomaly_flags": []}
    
    with get_session() as session:
        line_item_match_already_mapped = session.exec(
            select(LineItemMatch)
            .where(LineItemMatch.invoice_line_item_id.in_([item.invoice_line_item_id for item in invoice_line_items])) # type: ignore
        ).all()

    if len(line_item_match_already_mapped) > 0:
        logger.info(f"{len(line_item_match_already_mapped)} items already mapped to contract")

    # UNFILTER ALREADY MAPPED ITEMS & CREATE CONTRACT CANDIDATES     
    line_item_id_already_mapped = [item.invoice_line_item_id for item in line_item_match_already_mapped]
    unresolved = [
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
    
    # EXACT MATCH
    for inv_line in list(unresolved):
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
            unresolved.remove(inv_line)

    # FUZZY MATCH
    for inv_line in list(unresolved):
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
            unresolved.remove(inv_line)
    
    # VECTOR SEARCH
    matched_vector = _vector_match(
        unresolved, 
        contract_candidates,
        invoice.supplier_name,
        settings.thresholds.pipeline_vector_match_min,
    )
    
    for inv_line_id, (contract_line, score) in matched_vector.items():
        if contract_line:
            results.append(
                LineItemMatch(
                    contract_line_item_id=contract_line.contract_line_item_id, 
                    invoice_line_item_id=inv_line_id,
                    match_method=Method.vector,
                    match_score=score
                )
            )
            inv_line = next(inv_line for inv_line in unresolved if inv_line.invoice_line_item_id == inv_line_id)
            unresolved.remove(inv_line)

    # LLM MATCH    
    matched_llm = await _llm_match(unresolved, contract_candidates)
    
    for inv_line_desc, mapping in matched_llm.items():
        if mapping:
            contract = next(c for c in contract_candidates if c.product_service_name == mapping)
            inv_line = next(inv_line for inv_line in unresolved if inv_line.description == inv_line_desc)
            
            results.append(
                LineItemMatch(
                    contract_line_item_id=contract.contract_line_item_id, 
                    invoice_line_item_id=inv_line.invoice_line_item_id,
                    match_method=Method.llm,
                    match_score=0.6
                )
            )
            unresolved.remove(inv_line)
            
    # DB WRITE
    if results:
        with get_session() as session:
            try:
                session.add_all(results)
                session.commit()
            except SQLAlchemyError as e:
                raise PipelineRepositoryError(invoice.invoice_id) from e
            
    # ANOMALY FLAGS
    flags = []
    
    if results:
        non_exact = [line_match for line_match in results if line_match.match_method != Method.exact]
        
        if non_exact:
            flag_yellow = AnomalyFlag(
                anomaly_report_id=None,
                invoice_id=invoice.invoice_id,
                anomaly_name="not_exact_match",
                anomaly_severity=Severity.yellow,
                anomaly_source=Source.contract_matching,
                anomaly_deviation=None,
                anomaly_notes=None # don't comment it, I add pydantic model for that after check
            )
            flags.append(flag_yellow)
            
    if unresolved:
        flag_red = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice.invoice_id,
            anomaly_name="unmatched_invoice_line_item",
            anomaly_severity=Severity.red,
            anomaly_source=Source.contract_matching,
            anomaly_deviation=None,
            anomaly_notes=None # don't comment it, I add pydantic model for that after check
        )
        flags.append(flag_red) 
        
    return {
        "anomaly_flags": flags
    }
            


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
        invoice_line_items: list[InvoiceLineItem], 
        contract_line_items: list[ContractLineItem],
        supplier_name: str,
        confidence_threshold: float = settings.thresholds.pipeline_vector_match_min,
    ) -> dict[UUID, tuple[ContractLineItem | None, float]]:
    """
    Perform vector match of contract product / service name on invoice line item description.
    """
    results: dict[UUID, tuple[ContractLineItem | None, float]] = dict()
    contract_ids = [str(c.contract_line_item_id) for c in contract_line_items]
    
    if not contract_ids:
        for item in invoice_line_items:
            results[item.invoice_line_item_id] = (None, 0.0)
        return results    
    
    invoice_line_descriptions = [line.description for line in invoice_line_items]
    
    if not invoice_line_descriptions:
        for item in invoice_line_items:
            results[item.invoice_line_item_id] = (None, 0.0)
        return results 
    

    collection = get_collection()
    query_result = query_similar(
        collection,
        invoice_line_descriptions,
        supplier_name,
        contract_ids,
    )
    
    if query_result["distances"] is None:
        for item in invoice_line_items:
            results[item.invoice_line_item_id] = (None, 0.0)
        return results 
    

    for i, (id, distance) in enumerate(zip(query_result["ids"], query_result["distances"])):
    
        matched_id = id[0]
        distance = distance[0]
        similarity = 1 - distance
        
        if similarity >= confidence_threshold:
            matched_item = next(
                (c for c in contract_line_items if str(c.contract_line_item_id) == matched_id),
                None
            )
            item = invoice_line_items[i]
            
            results[item.invoice_line_item_id] = (matched_item, similarity)
        
    return results



async def _llm_match(
        invoice_line_item: list[InvoiceLineItem], 
        contract_line_items: list[ContractLineItem],
        prompt: str = CONTRACT_MATCHING_PROMPT,
    ) -> dict[str, str | None]:
    """
    Last instance of cascade to map unresolved cases.
    Sends them contract line items names to LLM.
    LLM responds with json with invoice line item description assigned to contract line item name.
    If it unsures where line item description belongs to, it maps it to null.
    NOTE: confidence score for llm match is always 0.6.
    
    Args:
        invoice_line_item: InvoiceLineItem object
        contract_line_items: list of ContractLineItem objects
        ollama_url: ollama url of local model
        model_name: model name performing validation
        prompt: system prompt to map invoice line item description
    
    Returns:
        ContractLineItem assigned to invoice line item description or None
    """
    
    contract_names = [item.product_service_name for item in contract_line_items]
    inv_line_descriptions = [item.description for item in invoice_line_item]
    
    prompt_formatted = prompt.format(
        invoice_line_item_description=[item.description for item in invoice_line_item],
        product_service_names=contract_names
    )
    
    response_dict = await call_local_llm(prompt_formatted, expect_json=True) 
    
    if not isinstance(response_dict, dict):
        raise ValueError("LLM return is not in dict format!")
    
    results = dict()
    for inv_line_desc, mapping in response_dict.items():
        if (
            inv_line_desc in inv_line_descriptions
            and mapping 
            and mapping in contract_names # hallucination check
            ):
            results[inv_line_desc] = mapping
            
    return results
