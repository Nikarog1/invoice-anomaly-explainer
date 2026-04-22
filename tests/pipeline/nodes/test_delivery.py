from datetime import datetime, timezone
from uuid import uuid4, UUID

from pipeline.nodes.delivery import delivery 
from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, AnomalyReport, Severity, Source
from schemas.invoice import Invoice
from schemas.junction import LineItemMatch, Method



def test_delivery_returns_expected_output(fake_session):
    invoice = Invoice(
        invoice_number="01234",
        supplier_name="suppl1",
        total_amount=1000.0
    )
    invoice_id = invoice.invoice_id
    
    anomaly_flags = [
        AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="anomaly_name_1",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.completeness_check_ingestion
        ),
        AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="anomaly_name_2",
            anomaly_severity=Severity.red,
            anomaly_source=Source.statistical_vs_contract,
        ),
    ]
    
    line_item_matches = [
        LineItemMatch(
            contract_line_item_id=uuid4(), 
            invoice_line_item_id=uuid4(), 
            match_method=Method.exact,
            match_score=None,
        ),
        LineItemMatch(
            contract_line_item_id=uuid4(), 
            invoice_line_item_id=uuid4(), 
            match_method=Method.vector,
            match_score=0.85,
        ),
        LineItemMatch(
            contract_line_item_id=uuid4(), 
            invoice_line_item_id=uuid4(), 
            match_method=Method.llm,
            match_score=0.6,
        ), 
    ]
    
    agent_explanation = "Some useful explanation about 2 detected anomalies."
    explanation_datetime = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "anomaly_flags": anomaly_flags,
        "line_item_matches": line_item_matches,
        "agent_explanation": agent_explanation,
        "explanation_datetime": explanation_datetime, 
    } # type: ignore[typeddict-item]
    
    output = delivery(state)
    
    assert isinstance(output["anomaly_report_id"], UUID)
    
    from sqlmodel import select

    reports = fake_session.exec(select(AnomalyReport)).all()
    assert len(reports) == 1
    assert reports[0].anomaly_report_id == output["anomaly_report_id"]

    flags = fake_session.exec(select(AnomalyFlag)).all()
    assert len(flags) == 2
    assert all(f.anomaly_report_id == output["anomaly_report_id"] for f in flags)

    matches = fake_session.exec(select(LineItemMatch)).all()
    assert len(matches) == 3
    