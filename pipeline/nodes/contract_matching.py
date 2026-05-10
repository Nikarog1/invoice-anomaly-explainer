from uuid import UUID

from chromadb.errors import ChromaError
import httpx

from rapidfuzz import fuzz

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import select

from config.prompts.contract_matching import CONTRACT_MATCHING_PROMPT
from config.settings import settings

from core.exceptions import PipelineRepositoryError, PipelineStateError
from data.llm_client import call_local_llm
from core.logging import get_logger

from data.sqlite import get_session
from data.vector_store import get_collection, query_similar

from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, MatchedPair, NotExactMatchNotes, Severity, Source, UnresolvedMatchNotes
from schemas.contract import ContractLineItem
from schemas.invoice import InvoiceLineItem
from schemas.junction import LineItemMatch, Method

logger = get_logger(__name__)



async def contract_matching(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    """
    Match invoice line items to contract line items via cascade.

    Loads existing LineItemMatch rows from SQL and skips those line items.
    Runs cascade on the rest: exact -> fuzzy -> vector -> LLM.
    Each stage operates only on items unresolved by previous stages.

    New matches written to LineItemMatch in this node (durable factual data).
    Two AnomalyFlags possible:
        - yellow not_exact_match: some matches required fuzzy/vector/LLM, need review.
        - red unmatched_invoice_line_item: cascade exhausted without match.

    Vector and LLM stages are skipped on Ollama/ChromaDB connectivity failure
    without aborting the node.

    Raises:
        PipelineStateError: if invoice, invoice_line_items, or contract_summary missing.
        PipelineRepositoryError: if SQL write fails.
    """
    logger.info("Running contract_matching")
    
    invoice = state["invoice"]
    invoice_line_items = state["invoice_line_items"]
    contract_summary = state["contract_summary"]
    
    if (
        invoice is None
        or invoice_line_items is None
        or contract_summary is None
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
    logger.info(f"Exact match resolved {len(results)}/{(len(results)+len(unresolved))} line items")
    
    # FUZZY MATCH
    fuzzy_resolved = []
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
            fuzzy_resolved.append(
                MatchedPair(
                    invoice_description=inv_line.description,
                    matched_contract_name=matched_fuzzy.product_service_name,
                    score=score
                )
            )
            unresolved.remove(inv_line)
    logger.info(f"Fuzzy match resolved {len(fuzzy_resolved)} additional line items")
    
    # VECTOR SEARCH
    vector_resolved = []
    try:
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
                vector_resolved.append(
                    MatchedPair(
                        invoice_description=inv_line.description,
                        matched_contract_name=contract_line.product_service_name,
                        score=score
                    )
                )
                unresolved.remove(inv_line)
        logger.info(f"Vector search resolved {len([r for r in results if r.match_method == Method.vector])} additional line items")
    except (ConnectionError, ChromaError, httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
        logger.warning(f"Skipping vector search, infrastructure unavailable: {e}")
    
    # LLM MATCH
    llm_resolved = []
    try:
        matched_llm = await _llm_match(unresolved, contract_candidates)
        
        for inv_line_id, con_line_name in matched_llm.items():
            if con_line_name:
                contract = next(c for c in contract_candidates if c.product_service_name == con_line_name)
                inv_line = next(inv_line for inv_line in unresolved if inv_line.invoice_line_item_id == inv_line_id)
                
                results.append(
                    LineItemMatch(
                        contract_line_item_id=contract.contract_line_item_id, 
                        invoice_line_item_id=inv_line.invoice_line_item_id,
                        match_method=Method.llm,
                        match_score=0.6
                    )
                )
                llm_resolved.append(
                    MatchedPair(
                        invoice_description=inv_line.description,
                        matched_contract_name=contract.product_service_name,
                        score=0.6
                    )
                )
                unresolved.remove(inv_line)
        logger.info(f"LLM match resolved {len([r for r in results if r.match_method == Method.llm])} additional line items")
        
    except (ConnectionError, httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
        logger.warning(f"Skipping LLM match, Ollama unavailable: {e}")
        
            
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
    
    if fuzzy_resolved or vector_resolved or llm_resolved:
            
        notes_yellow = NotExactMatchNotes(
            fuzzy_resolved=fuzzy_resolved,
            vector_resolved=vector_resolved,
            llm_resolved=llm_resolved,
        )
        flag_yellow = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice.invoice_id,
            anomaly_name="not_exact_match",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.contract_matching,
            anomaly_deviation=None,
            anomaly_notes=notes_yellow.model_dump_json(),
        )
        flags.append(flag_yellow)
        logger.info(
            f"Yellow flag: {len(notes_yellow.fuzzy_resolved)+len(notes_yellow.vector_resolved)+len(notes_yellow.llm_resolved)} "
            f"non-exact matches (fuzzy={len(notes_yellow.fuzzy_resolved)}, "
            f"vector={len(notes_yellow.vector_resolved)}, llm={len(notes_yellow.llm_resolved)})"
        )
            
    if unresolved:
        notes_red = UnresolvedMatchNotes(unresolved_invoice_line_items=[u.description for u in unresolved])
        flag_red = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice.invoice_id,
            anomaly_name="unmatched_invoice_line_item",
            anomaly_severity=Severity.red,
            anomaly_source=Source.contract_matching,
            anomaly_deviation=None,
            anomaly_notes=notes_red.model_dump_json(),
        )
        flags.append(flag_red)
        logger.info(
            f"Red anomaly flag raised! "
            f"unresolved invoice line items={len(notes_red.unresolved_invoice_line_items)}"
        )
        
    if not flags:
        logger.info("No anomaly flag raised")
        
    return {
        "anomaly_flags": flags
    }
            


def _exact_match(invoice_line_item: InvoiceLineItem, contract_line_items: list[ContractLineItem]) -> ContractLineItem | None:
    """
    Compare invoice description to contract product_service_name with raw equality.
    Returns the first matching ContractLineItem, or None.
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
    Find best fuzzy match for invoice description across contract line items.

    Uses rapidfuzz token_set_ratio (handles word reordering and extra/missing tokens).
    Returns the best-scoring contract item if score >= threshold, else (None, 0.0).
    Ties broken by first-seen order.
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
    Find top-1 semantic match for each invoice description via ChromaDB.

    Single batched query to the vector collection, scoped to the supplier's contract
    line items. Distances converted to similarity (1 - cosine_distance).
    Returns mapping invoice_line_item_id -> (contract_item or None, score).
    Items below threshold mapped to (None, 0.0).
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
        invoice_line_items: list[InvoiceLineItem], 
        contract_line_items: list[ContractLineItem],
        prompt: str = CONTRACT_MATCHING_PROMPT,
    ) -> dict[str, str | None]:
    """
    Send unresolved invoice descriptions and contract names to local LLM.

    LLM returns a JSON dict mapping each invoice line item id to a contract name or null.
    Hallucinated names (not in contract list) and unknown keys (not in input)
    are filtered out. Confidence fixed at 0.6 for any successful LLM match.

    Returns dict description -> matched_name. Empty dict on no matches.
    """
    
    contract_names = [item.product_service_name for item in contract_line_items]
    inv_line_ids_descriptions = {str(item.invoice_line_item_id): item.description for item in invoice_line_items}
    
    prompt_formatted = prompt.format(
        invoice_line_items=inv_line_ids_descriptions,
        product_service_names=contract_names
    )
    
    response_dict = await call_local_llm(prompt_formatted, expect_json=True) 
    
    if not isinstance(response_dict, dict):
        raise ValueError("LLM return is not in dict format!")
    
    results = dict()
    for inv_id, con_line_name in response_dict.items():
        if (
            inv_id in inv_line_ids_descriptions
            and con_line_name 
            and con_line_name in contract_names # hallucination check
            ):
            results[UUID(inv_id)] = con_line_name
            
    return results
