from datetime import timezone
from uuid import uuid4

import pytest
from unittest.mock import AsyncMock, patch

from core.exceptions import ExplanationFailedError, PipelineStateError

from pipeline.agents.agent_explanation import explanation
from pipeline.agents.models import ExplanationPlan
from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.contract import ContractSummary, DegradationReason as DR_Contract
from schemas.history import DegradationReason as drh, HistoricalSummary
from schemas.invoice import Invoice, InvoiceLineItem



def _generate_state():
    invoice = Invoice(
        invoice_number="012345",
        supplier_name="suppl1",
        total_amount=1000.0
    )
    invoice_id = invoice.invoice_id
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=400.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=600.0),
    ]
    contract_summary = ContractSummary(contracts=[], is_degraded=True, degradation_reason=DR_Contract.no_contract)
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=0,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=[],
        is_degraded=True,
        degradation_reason=drh.no_history,
    )
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": invoice_line_items,
        "contract_summary": contract_summary,
        "historical_summary": historical_summary
    } # type: ignore[typeddict-item]
    
    return state
    
async def test_explanation_raises_exception():
    state: PipelineState = {
        "invoice_id": uuid4(),
        "invoice": None,
        "invoice_line_items": None,
        "contract_summary": None,
        "historical_summary": None
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        await explanation(state)
        

async def test_explanation_no_flags_returns_expected_output():
    state = _generate_state()
    state["anomaly_flags"] = []
    
    output = await explanation(state)
    report = output["agent_report"]
    
    assert report.invoice_id == state["invoice_id"]
    assert report.anomalies_count == 0
    assert report.agent_explanation == "No anomaly found, everything is fine."
    assert report.explanation_date
    assert report.explanation_date.tzinfo == timezone.utc
    

async def test_explanation_raises_exception_step1():
    state = _generate_state()
    state["anomaly_flags"] = [
        AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=state["invoice_id"],
            anomaly_name="line_amount_deviation",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None,
            anomaly_notes=None
        )
    ]
    
    fake_response = {"missing": "fields"}
    with patch(
        "pipeline.agents.agent_explanation.call_local_llm",
        new_callable=AsyncMock,
        return_value=fake_response,
    ):
        with pytest.raises(ExplanationFailedError):
            await explanation(state)
    
async def test_explanation_raises_exception_step2():
    state = _generate_state()
    state["anomaly_flags"] = [
        AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=state["invoice_id"],
            anomaly_name="line_amount_deviation",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None,
            anomaly_notes=None
        )
    ]
    
    fake_structured_output = ExplanationPlan(
        summary="some summary",
        top_concerns=[],
        degradation_caveats=[],
        flag_groupings=[]
    )
    fake_response = 12345
    
    with (
        patch(
            "pipeline.agents.agent_explanation._get_structured_explanation",
            new_callable=AsyncMock,
            return_value=fake_structured_output,
        ),
        patch(
            "pipeline.agents.agent_explanation.call_local_llm",
            new_callable=AsyncMock,
            return_value=fake_response,
        ),
    ):
        with pytest.raises(ExplanationFailedError):
            await explanation(state)
            

async def test_explanation_full_pipeline_returns_expected_output():
    state = _generate_state()
    state["anomaly_flags"] = [
        AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=state["invoice_id"],
            anomaly_name="line_amount_deviation",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None,
            anomaly_notes=None
        )
    ]
    
    fake_structured_output = ExplanationPlan(
        summary="some summary",
        top_concerns=[],
        degradation_caveats=[],
        flag_groupings=[]
    )
    fake_plain_explanation = "some explanation"
    
    with (
        patch(
            "pipeline.agents.agent_explanation._get_structured_explanation",
            new_callable=AsyncMock,
            return_value=fake_structured_output,
        ),
        patch(
            "pipeline.agents.agent_explanation._get_plain_explanation",
            new_callable=AsyncMock,
            return_value=fake_plain_explanation,
        ),
    ):
        output = await explanation(state)
        
    report = output["agent_report"]
    assert report.invoice_id == state["invoice_id"]
    assert report.anomalies_count == 1
    assert report.agent_explanation == "some explanation"
    assert report.explanation_date
    assert report.explanation_date.tzinfo == timezone.utc
    

async def test_explanation_suceeds_on_second_try_returns_expected():
    state = _generate_state()
    state["anomaly_flags"] = [
        AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=state["invoice_id"],
            anomaly_name="line_amount_deviation",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None,
            anomaly_notes=None
        )
    ]
    
    fake_structured_output = ExplanationPlan(
        summary="some summary",
        top_concerns=[],
        degradation_caveats=[],
        flag_groupings=[]
    )
    llm_mock = AsyncMock(side_effect=[
        {"bad": "response"},
        fake_structured_output,
    ])
    
    fake_plain_explanation = "some explanation"

    with (
        patch(
            "pipeline.agents.agent_explanation.call_local_llm",
            new=llm_mock,
        ),
        patch(
            "pipeline.agents.agent_explanation._get_plain_explanation",
            new_callable=AsyncMock,
            return_value=fake_plain_explanation,
        ),
    ):
        output = await explanation(state)
        
    report = output["agent_report"]
    assert report
    assert llm_mock.call_count == 2