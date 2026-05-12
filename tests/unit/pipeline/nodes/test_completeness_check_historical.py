from datetime import date
import json
from uuid import uuid4

import pytest

from core.exceptions import PipelineStateError
from pipeline.nodes.completeness_check_historical import completeness_check_historical
from pipeline.state import PipelineState
from schemas.anomaly import Severity, Source
from schemas.history import HistoricalSummary
from schemas.invoice import Invoice, InvoiceLineItem


    
def test_completeness_check_historical_returns_expected_output():
    invoice = Invoice(
        invoice_number="num1",
        supplier_name="suppl1",
        buyer_name="our_company",
        total_amount=1000.0,
        issue_date=date(2026, 4, 1),
        currency="EUR",
        payment_details=None,
        invoice_metadata={
            "Current invoice field": "abc",
            "Common field": "123",
        }
    )
    invoice_id = invoice.invoice_id
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0, quantity=100, unit_price=10, amount_net=None),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0, quantity=100, unit_price=10, amount_net=None),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=50,
        fields_seen=set([
            "invoice_number", "supplier_name", "total_amount", "issue_date", "due_date", "invoice_metadata",
            'invoice_line_item_id', "invoice_id", "description", "amount_gross", "vat_rate",
        ]),
        metadata_keys_seen=set(["Hist invoice field", "Common field"]),
        line_item_stats=[],
        is_degraded=False,
        degradation_reason=None,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    result = completeness_check_historical(state)
    
    flag = result["anomaly_flags"][0]
    
    assert flag.invoice_id == invoice_id
    assert flag.anomaly_name == Source.completeness_check_historical
    assert flag.anomaly_severity == Severity.yellow
    assert flag.anomaly_source == Source.completeness_check_historical
    assert flag.anomaly_deviation is None
    assert flag.anomaly_notes is not None
    
    notes = json.loads(flag.anomaly_notes)
    
    assert len(notes["missing_universal_fields"]) == 2
    assert "due_date" in notes["missing_universal_fields"]
    assert "vat_rate" in notes["missing_universal_fields"]
    assert "supplier_name" not in notes["missing_universal_fields"]
    assert "description" not in notes["missing_universal_fields"]
    
    assert len(notes["new_universal_fields"]) == 4
    assert "currency" in notes["new_universal_fields"]
    assert "unit_price" in notes["new_universal_fields"]
    assert "buyer_name" in notes["new_universal_fields"]
    assert "payment_details" not in notes["new_universal_fields"]
    assert "amount_net" not in notes["new_universal_fields"]
    assert "supplier_name" not in notes["new_universal_fields"]
    assert "description" not in notes["new_universal_fields"]
    
    assert len(notes["missing_metadata_keys"]) == 1
    assert "Hist invoice field" in notes["missing_metadata_keys"]
    assert "Common field" not in notes["missing_metadata_keys"]
    
    assert len(notes["new_metadata_keys"]) == 1
    assert "Current invoice field" in notes["new_metadata_keys"]
    assert "Common field" not in notes["new_metadata_keys"]
    

def test_completeness_check_historical_returns_empty_list():
    invoice = Invoice(
        invoice_number="num1",
        supplier_name="suppl1",
        total_amount=1000.0,
        issue_date=date(2026, 4, 1),
        invoice_metadata={
            "Common field": "123",
        }
    )
    invoice_id = invoice.invoice_id
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=50,
        fields_seen=set([
            "invoice_number", "supplier_name", "total_amount", "issue_date", "invoice_metadata",
            'invoice_line_item_id', "invoice_id", "description", "amount_gross",
        ]),
        metadata_keys_seen=set(["Common field"]),
        line_item_stats=[],
        is_degraded=False,
        degradation_reason=None,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    result = completeness_check_historical(state)
    
    assert result["anomaly_flags"] == []
    

def test_completeness_check_historical_raises_exception():
    
    state: PipelineState = {
        "invoice_id": uuid4(),
        "invoice": None,
        "invoice_line_items": None,
        "historical_summary": None,
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        completeness_check_historical(state)