from datetime import datetime, timezone
import json
from pydantic import ValidationError

from config.prompts.explanation import EXPLANATION_NARRATIVE_PROMPT, EXPLANATION_PLAN_PROMPT
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
    """
    Run two-step LLM agent to produce a plain-English anomaly explanation.

    Step 1: LLM analyzes flags and returns a structured plan (priorities, groupings).
    Step 2: LLM writes the user-facing narrative conditioned on the plan.

    When invoice has no anomaly flags, skips both LLM calls and returns a templated
    "clean invoice" report.

    Returns:
        {"agent_report": AnomalyReport} — explanation string stored in agent_explanation field.
        Report persistence handled by the delivery node.

    Raises:
        PipelineStateError: if any required state field is missing.
        ExplanationFailedError: if either LLM step fails after retries.
    """
    
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
    
    logger.info("Step 1: requesting structured plan")
    structured_output = await _get_structured_explanation(explanation_context, EXPLANATION_PLAN_PROMPT)
    
    logger.info("Step 2: requesting narrative")
    plain_explanation = await _get_plain_explanation(structured_output, EXPLANATION_NARRATIVE_PROMPT)
    
    logger.info(f"Explanation produced ({len(plain_explanation)} chars)")
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
    Build the invoice presentation passed to the explanation agent.

    Strips internal IDs and metadata; keeps only fields the agent uses
    to describe the invoice to the user (number, supplier, date, totals,
    line item descriptions and amounts).
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
    Convert AnomalyFlag rows into agent-shaped entries.

    Parses anomaly_notes JSON back into a dict so the agent receives structured
    data rather than raw strings. None notes preserved as None.
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
    Assemble the full input bundle for the explanation agent.

    Merges invoice presentation, degradation reasons from history and contract
    summaries, and parsed flag entries into a single ExplanationContext.
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
    """
    LLM step 1: produce structured analysis plan from explanation context.

    Sends context + ExplanationPlan schema to LLM. Validates response with Pydantic.
    On validation failure, retries once with the validation error fed back to the LLM.

    Raises:
        ExplanationFailedError: if both attempts fail validation.
    """
    base_prompt = prompt.format(
        context=explanation_context.model_dump_json(indent=2),
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
            logger.warning(f"Step 1 attempt {attempt + 1} failed validation: {e}")
    
    raise ExplanationFailedError(f"Step 1: {str(last_error)}")



async def _get_plain_explanation(
        structured_output: ExplanationPlan,
        prompt: str
) -> str:
    """
    LLM step 2: produce plain-English narrative from structured plan.

    Sends the analysis plan to LLM, expects free-form string response.
    Retries once on non-string response.

    Raises:
        ExplanationFailedError: if both attempts fail to return a string.
    """
    base_prompt = prompt.format(
        plan=structured_output.model_dump_json(indent=2),
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
        logger.warning(f"Step 2 attempt {attempt + 1} returned non-string")
        
    raise ExplanationFailedError("Step 2: LLM returned non-string response")

    
    