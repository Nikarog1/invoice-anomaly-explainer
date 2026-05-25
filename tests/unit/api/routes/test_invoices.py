from uuid import uuid4

from schemas.invoice import Invoice, InvoiceLineItem



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
    