from datetime import datetime, timezone
from uuid import uuid4

from sqlmodel import select

import pytest

from core.exceptions import PipelineStateError

from pipeline.nodes.delivery import delivery 
from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, AnomalyReport, Severity, Source



def test_delivery_returns_expected_output(fake_session) -> None:
    invoice_id = uuid4()
    anomaly_report = AnomalyReport(
        invoice_id=invoice_id,
        anomalies_count=2,
        agent_explanation="some explanation",
        explanation_date=datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    )
    
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
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "anomaly_flags": anomaly_flags,
        "agent_report": anomaly_report,
    } # type: ignore[typeddict-item]
    
    output = delivery(state)
    result = output["anomaly_report"]
    
    assert result == anomaly_report
    
    report = fake_session.exec(select(AnomalyReport)).first()
    assert report
    assert report.anomaly_report_id == anomaly_report.anomaly_report_id

    flags = fake_session.exec(select(AnomalyFlag)).all()
    assert len(flags) == 2
    assert all(f.anomaly_report_id == anomaly_report.anomaly_report_id for f in flags)
    

def test_delivery_raises_exception() -> None:
    state: PipelineState = {
        "invoice_id": uuid4(),
        "agent_report": None,
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        delivery(state)
    