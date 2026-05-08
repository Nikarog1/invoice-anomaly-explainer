from datetime import datetime, timezone
import json

from core.exceptions import ExplanationFailedError, PipelineStateError
from core.logging import get_logger

from data.llm_client import call_local_llm

from pipeline.agents.models import (
    ConcernEntry, ExplanationContext, ExplanationPlan, 
    FlagEntry, FlagGroup, InvoiceSummary, LineItemSummary,
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
    
    invoice_summary = _create_invoice_summary(invoice, invoice_line_items)
    flag_entries = _create_flag_entries(anomaly_flags)
    explanation_context = _create_explanation_context(
        invoice_summary,
        historical_summary,
        contract_summary,
        flag_entries,
    )
    
    structured_output = await _get_structured_explanation(explanation_context)
    plain_explanation = await _get_plain_explanation(structured_output)
    
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
        attempt: int = 0    
) -> ExplanationPlan:
    attempt += 1
    prompt = """
    some prompt with {context} and format of output {format} and 2 arguments {format_arg1} & {format_arg2},
    also with format of {severity} and {source}
    , will be added later
    """
    prompt_formatted = prompt.format(
        context=explanation_context,
        format=ExplanationPlan, # not sure about this one
        format_arg1=ConcernEntry, # not sure about this one
        format_arg2=FlagGroup, # not sure about this one
        severity=Severity, # not sure about this one
        source=Source, # not sure about this one
    )
    
    response_dict = await call_local_llm(prompt_formatted, expect_json=True)
    
    if (
        response_dict is None
        or not isinstance(response_dict, dict)
        or (
            not response_dict["summary"]
            or not response_dict["top_concerns"]
            or not response_dict["degradation_caveats"]
            or not response_dict["flag_groupings"]
        )
        or (
            not response_dict["top_concerns"][0]["anomaly_name"]
            or not response_dict["top_concerns"][0]["anomaly_severity"]
            or not response_dict["top_concerns"][0]["anomaly_source"]
            or not response_dict["top_concerns"][0]["reason"]
            or not response_dict["flag_groupings"][0]["theme"]
            or not response_dict["flag_groupings"][0]["flags"]
            or not response_dict["flag_groupings"][0]["flags"][0]["anomaly_name"]
            or not response_dict["flag_groupings"][0]["flags"][0]["anomaly_severity"]
            or not response_dict["flag_groupings"][0]["flags"][0]["anomaly_source"]
            or not response_dict["flag_groupings"][0]["flags"][0]["reason"]
        )
        and attempt < 2
    ):
        result = await _get_structured_explanation(explanation_context, attempt=attempt) # return back to llm? would need to adjust prompt?
        return result

    if attempt >= 2:
        raise ExplanationFailedError("Failed to receive requested output in step 1!")
    
    structured_explanation = ExplanationPlan(
        summary=response_dict["summary"],
        top_concerns=[
            ConcernEntry(
                anomaly_name=concern["anomaly_name"],
                anomaly_severity=concern["anomaly_severity"],
                anomaly_source=concern["anomaly_source"],
                reason=concern["reason"],
            )
            for concern in response_dict["top_concerns"]
        ],
        degradation_caveats=[
            caveat
            for caveat in response_dict["degradation_caveats"]
        ],
        flag_groupings=[
            FlagGroup(
                theme=group["theme"],
                flags=[
                    ConcernEntry(
                        anomaly_name=concern["anomaly_name"],
                        anomaly_severity=concern["anomaly_severity"],
                        anomaly_source=concern["anomaly_source"],
                        reason=concern["reason"],
                    )
                    for concern in group["flags"]    
                ]
            )
            for group in response_dict["flag_groupings"]
        ]
    )
    
    return structured_explanation



async def _get_plain_explanation(structured_output: ExplanationPlan) -> str:
    prompt = """
    some prompt with {structured_output} and format of return defined directly in prompt, will be added later
    """
    prompt_formatted = prompt.format(
        structured_output=structured_output,
    )
    
    response_str = await call_local_llm(prompt_formatted, expect_json=False)
    
    if not isinstance(response_str, str):
        raise ExplanationFailedError("Failed to return string in step 2!")
    
    return response_str

    
    