from datetime import date
import math
from uuid import uuid4

import pytest

from core.exceptions import InvoiceValueNotFoundError, PipelineStateError

from pipeline.nodes.load_past_invoices import load_past_invoices
from pipeline.state import PipelineState

from schemas.history import DegradationReason
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.supplier_config import SupplierConfig


def _generate_state(issue_date: date | None = date(2026, 4, 1)) -> PipelineState:
    invoice = Invoice(
        invoice_number="num1",
        supplier_name="suppl1",
        buyer_name="our_company",
        total_amount=1000.0,
        issue_date=issue_date
    )
    invoice_id = invoice.invoice_id
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0),
    ]
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": invoice_line_items
    } # type: ignore[typeddict-item]
    
    return state

def _generate_history(fake_session, issue_date: date, n_samples: int) -> None:
    historical_invoices = [
        Invoice(
            invoice_number="hist_num1",
            supplier_name="suppl1",
            buyer_name="our_company",
            total_amount=1000.0,
            issue_date=issue_date
        ),
        Invoice(
            invoice_number="hist_num2",
            supplier_name="suppl1",
            buyer_name="our_company",
            total_amount=1200.0,
            issue_date=issue_date
        ),
        Invoice(
            invoice_number="hist_num3",
            supplier_name="suppl1",
            buyer_name="our_company",
            total_amount=950.0,
            issue_date=issue_date
        ),
        Invoice(
            invoice_number="hist_num4",
            supplier_name="suppl1",
            buyer_name="our_company",
            total_amount=1000.0,
            issue_date=issue_date
        ),
    ]
    
    historical_invoices_items = [
        InvoiceLineItem(invoice_id=historical_invoices[0].invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=historical_invoices[0].invoice_id, description="item2", amount_gross=400.0),
        InvoiceLineItem(invoice_id=historical_invoices[1].invoice_id, description="item1", amount_gross=700.0),
        InvoiceLineItem(invoice_id=historical_invoices[1].invoice_id, description="item2", amount_gross=500.0),
        InvoiceLineItem(invoice_id=historical_invoices[2].invoice_id, description="item1", amount_gross=500.0),
        InvoiceLineItem(invoice_id=historical_invoices[2].invoice_id, description="item2", amount_gross=450.0),
        InvoiceLineItem(invoice_id=historical_invoices[3].invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=historical_invoices[3].invoice_id, description="item2", amount_gross=400.0),
    ]
    
    fake_session.add_all(historical_invoices[:n_samples])
    fake_session.add_all(historical_invoices_items[:n_samples * 2])
    fake_session.commit()
    

    
def test_load_past_invoices_no_history_returns_expected_output(fake_session) -> None:
    state = _generate_state()
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.is_degraded is True
    assert summary.degradation_reason == DegradationReason.no_history
    

def test_load_past_invoices_happy_path_returns_expected_output(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    _generate_history(fake_session, issue_date=date(2026, 3, 1), n_samples=n_samples)
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.supplier_name == "suppl1"
    assert summary.invoice_count == n_samples
    assert "invoice_number" in summary.fields_seen
    assert "supplier_name" in summary.fields_seen
    assert "buyer_name" in summary.fields_seen
    assert "total_amount" in summary.fields_seen
    assert "issue_date" in summary.fields_seen
    assert summary.is_degraded is False
    assert summary.degradation_reason is None
    
    assert len(summary.line_item_stats) == 2
    
    summary_item1 = [stats for stats in summary.line_item_stats if stats.description == "item1"][0]
    assert summary_item1.description == "item1"
    assert math.isclose(summary_item1.mean_amount, 600.0, rel_tol=1e-9)
    assert math.isclose(summary_item1.stddev_amount, 81.6496, rel_tol=1e-5) 
    assert summary_item1.n_samples == 4


def test_load_past_invoices_window_miss_returns_expected_output(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    _generate_history(fake_session, issue_date=date(2025, 1, 1), n_samples=n_samples)
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.is_degraded is True
    assert summary.degradation_reason == DegradationReason.window_miss
    

def test_load_past_invoices_thin_count_returns_expected_output(fake_session) -> None:
    state = _generate_state()
    n_samples = 2
    _generate_history(fake_session, issue_date=date(2026, 3, 1), n_samples=n_samples)
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.is_degraded is True
    assert summary.degradation_reason == DegradationReason.thin_count
    

def test_load_past_invoices_custom_config_overwrites_default(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    _generate_history(fake_session, issue_date=date(2026, 1, 1), n_samples=n_samples)
    
    custom_config = SupplierConfig(supplier_name="suppl1", min_history_date=date(2026, 3, 1), min_samples=5)
    fake_session.add(custom_config)
    fake_session.commit()
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.is_degraded is True
    assert summary.degradation_reason == DegradationReason.thin_count


def test_load_past_invoices_custom_config_min_sample_None_overwrites_default(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    _generate_history(fake_session, issue_date=date(2026, 1, 1), n_samples=n_samples)
    
    custom_config = SupplierConfig(supplier_name="suppl1", min_history_date=date(2026, 3, 1), min_samples=None)
    fake_session.add(custom_config)
    fake_session.commit()
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.is_degraded is True
    assert summary.degradation_reason == DegradationReason.window_miss


def test_load_past_invoices_custom_config_wider_window_overwrites_default(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    _generate_history(fake_session, issue_date=date(2025, 1, 1), n_samples=n_samples)
    
    custom_config = SupplierConfig(supplier_name="suppl1", min_history_date=date(2025, 1, 1), min_samples=None)
    fake_session.add(custom_config)
    fake_session.commit()
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert summary.is_degraded is False
    assert summary.degradation_reason is None
    

def test_load_past_invoices_issue_date_None_raises_error(fake_session) -> None:
    state = _generate_state(issue_date=None)

    with pytest.raises(InvoiceValueNotFoundError):
        load_past_invoices(state)


def test_load_past_invoices_invoice_None_raises_error(fake_session) -> None:
    state: PipelineState = {
        "invoice_id": uuid4(),
        "invoice": None,
        "invoice_line_items": None,
    } # type: ignore[typeddict-item]

    with pytest.raises(PipelineStateError):
        load_past_invoices(state)  


def test_load_past_invoices_metadata_keys_returned(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    issue_date = date(2026, 3, 1)
    _generate_history(fake_session, issue_date=issue_date, n_samples=n_samples)
    
    hist_invoice_metadata = Invoice(
        invoice_number="hist_num4",
        supplier_name="suppl1",
        buyer_name="our_company",
        total_amount=1000.0,
        issue_date=issue_date,
        invoice_metadata={
            "Meta_field1": "123",
            "Meta_field2": "0987",
        }
    )
    
    fake_session.add(hist_invoice_metadata)
    fake_session.commit()
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert "Meta_field1" in summary.metadata_keys_seen
    assert "Meta_field2" in summary.metadata_keys_seen
    

def test_load_past_invoices_metadata_keys_returns_empty_set(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    issue_date = date(2026, 3, 1)
    _generate_history(fake_session, issue_date=issue_date, n_samples=n_samples)
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    summary_meta_keys = summary.metadata_keys_seen
    assert isinstance(summary_meta_keys, set)
    assert len(summary_meta_keys) == 0
    

def test_load_past_invoices_fields_seen_doesnt_take_None_fields(fake_session) -> None:
    state = _generate_state()
    n_samples = 4
    issue_date = date(2026, 3, 1)
    _generate_history(fake_session, issue_date=issue_date, n_samples=n_samples)
    
    hist_invoice_None = Invoice(
        invoice_number="hist_num4",
        supplier_name="suppl1",
        buyer_name="our_company",
        total_amount=1000.0,
        issue_date=issue_date,
        currency=None,
        payment_details=None,
    )
    
    fake_session.add(hist_invoice_None)
    fake_session.commit()
    
    output = load_past_invoices(state)
    summary = output["historical_summary"]
    
    assert "currency" not in summary.fields_seen
    assert "payment_details" not in summary.fields_seen
    

    
