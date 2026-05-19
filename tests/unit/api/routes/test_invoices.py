from uuid import uuid4

from schemas.invoice import Invoice, InvoiceLineItem



def test_get_invoice_returns_dto_when_invoice_exists(client, fake_session):
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


def test_get_invoice_returns_404_when_invoice_missing(client):
    missing_id = uuid4()

    response = client.get(f"/invoices/{missing_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == f"Invoice {missing_id} not found"