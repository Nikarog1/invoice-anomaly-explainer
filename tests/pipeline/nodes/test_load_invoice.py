import pytest
from uuid import uuid4

from core.exceptions import InvoiceNotFoundError
from pipeline.state import PipelineState
from pipeline.nodes.load_invoice import load_invoice
from schemas.invoice import Invoice, InvoiceLineItem



def test_load_invoice_returns_expected_output(fake_session):
    invoice = Invoice(
        invoice_number="num1",
        supplier_name="suppl1",
        buyer_name="our_company",
        total_amount=1000.0
    )
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice.invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice.invoice_id, description="item2", amount_gross=400.0),
    ]
    
    fake_session.add(invoice)
    fake_session.add_all(invoice_line_items)
    fake_session.commit()
    
    state: PipelineState = {"invoice_id": invoice.invoice_id} # type: ignore[typeddict-item]
    
    output = load_invoice(state)

    assert output["invoice"].invoice_number == invoice.invoice_number
    assert output["invoice"].supplier_name == invoice.supplier_name
    assert output["invoice"].buyer_name == invoice.buyer_name
    assert output["invoice"].total_amount == invoice.total_amount
    
    assert len(output["invoice_line_items"]) == 2
    assert output["invoice_line_items"][0].description == invoice_line_items[0].description
    assert output["invoice_line_items"][0].amount_gross == invoice_line_items[0].amount_gross
    

def test_load_invoice_unfound_error_returns_errors(fake_session):
    state: PipelineState = {"invoice_id": uuid4()} # type: ignore[typeddict-item]
    
    with pytest.raises(InvoiceNotFoundError):
        load_invoice(state)
    
    

    
    

    