from datetime import datetime, timezone
import json
from pydantic import ValidationError

from core.exceptions import ExplanationFailedError, PipelineStateError
from core.logging import get_logger

from data.llm_client import call_local_llm

from pipeline.agents.models import (
    ExplanationContext, ExplanationPlan, 
    FlagEntry, InvoiceSummary, LineItemSummary,
)
from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, AnomalyReport, Severity, Source
from schemas.contract import ContractSummary
from schemas.history import HistoricalSummary
from schemas.invoice import Invoice, InvoiceLineItem

logger = get_logger(__name__)



async def explanation(state: PipelineState) -> dict[str, AnomalyReport]:
    logger.info("Running explanation")
    invoice = state["invoice"]
    invoice_line_items = state["invoice_line_items"]
    historical_summary = state["historical_summary"]
    contract_summary = state["contract_summary"]
    
    if (
        invoice is None
        or invoice_line_items is None
        or historical_summary is None
        or contract_summary is None
    ):
        raise PipelineStateError("invoice, and/or invoice_line_items, and/or "
                                 "historical_summary, and/or contract_summary"
        )
    anomaly_flags = state["anomaly_flags"]
    
    if len(anomaly_flags) == 0:
        logger.info("No anomalies, skipping LLM")
        report = AnomalyReport(
            invoice_id=invoice.invoice_id,
            anomalies_count=len(anomaly_flags),
            agent_explanation="No anomaly found, everything is fine.",
            explanation_date=datetime.now(tz=timezone.utc)
        )
        return {
            "agent_report": report
        }
    
    invoice_summary = _create_invoice_summary(invoice, invoice_line_items)
    flag_entries = _create_flag_entries(anomaly_flags)
    explanation_context = _create_explanation_context(
        invoice_summary,
        historical_summary,
        contract_summary,
        flag_entries,
    )
    
    structured_output = await _get_structured_explanation(explanation_context, prompt="will be added")
    plain_explanation = await _get_plain_explanation(structured_output, prompt="will be added")
    
    report = AnomalyReport(
        invoice_id=invoice.invoice_id,
        anomalies_count=len(anomaly_flags),
        agent_explanation=plain_explanation,
        explanation_date=datetime.now(tz=timezone.utc)
    )
    
    return {
        "agent_report": report
    }



def _create_invoice_summary(
        invoice: Invoice,
        invoice_line_items: list[InvoiceLineItem],  
) -> InvoiceSummary:
    """
    Create invoice summary for explanation agent.
    
    Args:
        invoice: Invoice object
        invoice_line_items: list of InvoiceLineItem objects
        
    Returns:
        InvoiceSummary object
    """
    line_item_summary = [
        LineItemSummary(
            description=line.description,
            unit_price=line.unit_price,
            quantity=line.quantity,
            vat_rate=line.vat_rate,
            amount_gross=line.amount_gross,
        )
        for line in invoice_line_items
    ]
    invoice_summary = InvoiceSummary(
        invoice_number=invoice.invoice_number,
        supplier_name=invoice.supplier_name,
        issue_date=invoice.issue_date,
        total_amount=invoice.total_amount,
        currency=invoice.currency,
        line_items=line_item_summary
    )
    return invoice_summary



def _create_flag_entries(flags: list[AnomalyFlag]) -> list[FlagEntry]:
    """
    Create anomaly flag entries for explanation agent.
    
    Args:
        flags: list of AnomalyFlag objects or empty list
        
    Returns:
        list of FlagEntry objects
    """
    flag_entries = [
        FlagEntry(
            anomaly_name=flag.anomaly_name,
            anomaly_severity=flag.anomaly_severity,
            anomaly_source=flag.anomaly_source,
            anomaly_notes=json.loads(flag.anomaly_notes) if flag.anomaly_notes else None
        )
        for flag in flags
    ]
    return flag_entries



def _create_explanation_context(
        invoice_summary: InvoiceSummary,
        historical_summary: HistoricalSummary,
        contract_summary: ContractSummary,
        flag_entries: list[FlagEntry],
) -> ExplanationContext:
    """
    Create explanation context for explanation agent.
    
    Args:
        invoice_summary: InvoiceSummary object
        historical_summary: HistoricalSummary object
        contract_summary: ContractSummary object
        flag_entries: list of FlagEntry objects
        
    Returns:
        ExplanationContext object
    """
    explanation_context = ExplanationContext(
        invoice_summary=invoice_summary,
        historical_degradation=historical_summary.degradation_reason,
        contract_degradation=contract_summary.degradation_reason,
        anomaly_flags=flag_entries
    )
    return explanation_context



async def _get_structured_explanation(
        explanation_context: ExplanationContext, 
        prompt: str
) -> ExplanationPlan: # type: ignore
    prompt = """
    some prompt with {context} and format of output {output_schema},
    also with format of {severities} and {sources}
    , will be added later
    """
    base_prompt = prompt.format(
        context=explanation_context.model_dump_json(indent=2),
        output_schema=ExplanationPlan.model_json_schema(),
        severities=[s.value for s in Severity],
        sources=[s.value for s in Source],
    )
    
    last_error = None
    
    for attempt in range(2):
        prompt = base_prompt
        
        if attempt > 0:
            prompt = (
                base_prompt 
                + f"\n Previous attempt returned {last_error} validation error. Please return valid JSON matching schema"
            )
        response = await call_local_llm(prompt, expect_json=True)
        
        try:
            return ExplanationPlan.model_validate(response)
        except ValidationError as e:
            last_error = e
    
    raise ExplanationFailedError(f"Step 1: {str(last_error)}")



async def _get_plain_explanation(
        structured_output: ExplanationPlan,
        prompt: str
) -> str:
    prompt = """
    some prompt with {structured_output} and format of return defined directly in prompt, will be added later
    """
    base_prompt = prompt.format(
        structured_output=structured_output,
    )
    
    for attempt in range(2):
        prompt = base_prompt
        
        if attempt > 0:
            prompt = (
                base_prompt 
                + f"\n Previous attempt did not return string. Please return valid string."
            )
        response = await call_local_llm(prompt, expect_json=False)
        
        if isinstance(response, str):
            return response
        
    raise ExplanationFailedError("Step 2: LLM returned non-string response")

    
    