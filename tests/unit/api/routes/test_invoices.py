from datetime import datetime, timezone
import json
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4, UUID

import pytest

from schemas.anomaly import AnomalyFlag, AnomalyReport, Severity, Source
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.jobs import AnalysisJob



def _generate_invoice_with_lines() -> tuple[Invoice, InvoiceLineItem]:
    invoice = Invoice(
        invoice_number="12345",
        supplier_name="suppl1",
        buyer_name="our company",
        total_amount=1000.0,
        currency="EUR",
        invoice_metadata={},
    )
    invoice_line_item = InvoiceLineItem(
        invoice_id=invoice.invoice_id,
        description="item1",
        amount_gross=500.0,
    )
    return invoice, invoice_line_item

def _generate_analysis_job(
        invoice_id: UUID, 
        status: str, 
        anomaly_report_id: UUID = uuid4(), 
        error_message: str | None = None
) -> AnalysisJob:
    return AnalysisJob(
        anomaly_report_id=anomaly_report_id,
        invoice_id=invoice_id,
        status=status,
        error_message=error_message,
    )

    

def test_get_invoice_returns_dto_when_invoice_exists(client, fake_session) -> None:
    invoice, invoice_line_item = _generate_invoice_with_lines()
    fake_session.add(invoice)
    fake_session.add(invoice_line_item)
    fake_session.commit()

    response = client.get(f"/invoices/{invoice.invoice_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["invoice_number"] == "12345"
    assert body["supplier_name"] == "suppl1"
    assert len(body["line_items"]) == 1
    assert body["line_items"][0]["description"] == "item1"


def test_get_invoice_returns_404_when_invoice_missing(client) -> None:
    missing_id = uuid4()

    response = client.get(f"/invoices/{missing_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == f"Invoice {missing_id} not found"
    


def test_get_anomaly_report_returns_404_when_invoice_missing(client) -> None:
    missing_id = uuid4()

    response = client.get(f"/invoices/{missing_id}/report")

    assert response.status_code == 404
    assert response.json()["detail"] == f"Invoice {missing_id} not found"


def test_get_anomaly_report_no_latest_job_empty_response(client, fake_session) -> None:
    invoice, _ = _generate_invoice_with_lines()
    fake_session.add(invoice)
    fake_session.commit()

    response = client.get(f"/invoices/{invoice.invoice_id}/report")
    
    assert response.status_code == 200
    
    response_json = response.json()
    assert response_json["status"] == "not_analyzed"
    assert response_json["report"] is None
    assert response_json["error_message"] is None
    

def test_get_anomaly_report_returns_failed_status_with_error_message (client, fake_session) -> None:
    invoice, _ = _generate_invoice_with_lines()
    job = _generate_analysis_job(invoice.invoice_id, status="failed", error_message="Some message")
    
    fake_session.add(invoice)
    fake_session.add(job)
    fake_session.commit()

    response = client.get(f"/invoices/{invoice.invoice_id}/report")
    
    assert response.status_code == 200
    
    response_json = response.json()
    assert response_json["status"] == "failed"
    assert response_json["report"] is None
    assert response_json["error_message"] == "Some message"
    

def test_get_anomaly_report_raises_exception_successful_response_no_report(client, fake_session) -> None:
    invoice, _ = _generate_invoice_with_lines()
    job = _generate_analysis_job(invoice.invoice_id, status="succeeded")
    
    fake_session.add(invoice)
    fake_session.add(job)
    fake_session.commit()

    with pytest.raises(RuntimeError):
        client.get(f"/invoices/{invoice.invoice_id}/report")
        

def test_get_anomaly_report_happy_path(client, fake_session) -> None:
    invoice, _ = _generate_invoice_with_lines()
    invoice_id = invoice.invoice_id
    
    report = AnomalyReport(
        invoice_id=invoice_id,
        anomalies_count=2,
        agent_explanation="Some explanation",
        explanation_date=datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    )
    report_id = report.anomaly_report_id
    
    flags = [
        AnomalyFlag(
            anomaly_report_id=report_id,
            invoice_id=invoice_id,
            anomaly_name="name1",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.completeness_check_ingestion,
            anomaly_deviation=None,
            anomaly_notes=json.dumps({"key1": 0.1, "key2": "abc"})
        ),
        AnomalyFlag(
            anomaly_report_id=report_id,
            invoice_id=invoice_id,
            anomaly_name="name2",
            anomaly_severity=Severity.red,
            anomaly_source=Source.statistical_vs_contract,
            anomaly_deviation=None,
            anomaly_notes=None,
        ),
    ]
    latest_job = _generate_analysis_job(invoice_id, status="running", error_message="Some message")
    latest_success_job = _generate_analysis_job(invoice_id, status="succeeded", anomaly_report_id=report_id)
    
    fake_session.add(invoice)
    fake_session.add(report)
    fake_session.add_all(flags)
    fake_session.add(latest_job)
    fake_session.add(latest_success_job)
    fake_session.commit()

    response = client.get(f"/invoices/{invoice_id}/report")
    
    assert response.status_code == 200
    
    response_json = response.json()
    assert response_json["status"] == "analyzing"
    assert response_json["error_message"] is None
    
    response_report = response_json["report"]
    assert response_report["anomaly_report_id"] == str(report_id)
    assert response_report["invoice_id"] == str(invoice_id)
    assert response_report["anomalies_count"] == 2
    assert response_report["agent_explanation"] == "Some explanation"
    assert response_report["explanation_date"] == "2026-04-01T12:00:00"
    assert len(response_report["flags"]) == 2
    
    flag_0 = response_report["flags"][0]
    assert flag_0["name"] == "name1"
    assert flag_0["severity"] == "yellow"
    assert flag_0["source"] == "completeness_check_ingestion"
    assert flag_0["deviation"] is None
    assert len(flag_0["notes"]) == 2
    assert flag_0["notes"]["key1"] == 0.1
    


def test_upload_files_happy_path(client, tmp_path) -> None:
    csv_1 = b"invoice_number,supplier_name\n012345,Company1\n"
    csv_2 = b"invoice_number,supplier_name\n012346,Company1\n"
    
    mock_ingestion = AsyncMock()
    mock_ingestion.return_value = None
    
    with (
        patch("api.routes.invoices.run_ingestion", mock_ingestion),
        patch("api.routes.invoices.settings.csv_dir", tmp_path)
    ):
        response = client.post(
            f"/invoices", 
            files=[
                ("files", ("data_1.csv", csv_1, "text/csv")),
                ("files", ("data_2.csv", csv_2, "text/csv")),
            ]
        )
        
    assert response.status_code == 202
    
    response_json = response.json()
    assert isinstance(response_json["job_id"], str)
    assert response_json["status"] == "queued"
    

def test_upload_files_returns_404_wrong_file_format(client) -> None:
    txt = b"invoice_number,supplier_name\n012345,Company1\n"

    response = client.post(
        f"/invoices", 
        files=[
            ("files", ("data.txt", txt, "text/csv")),
        ]
    )
        
    assert response.status_code == 404
    assert response.json()["detail"] == "Invalid csv format: data.txt"