import json
import math
from uuid import uuid4

import pytest

from core.exceptions import PipelineStateError
from pipeline.nodes.statistical_vs_history import statistical_vs_history
from pipeline.state import PipelineState
from schemas.anomaly import Severity, Source
from schemas.history import DegradationReason, HistoricalSummary, LineItemStats
from schemas.invoice import InvoiceLineItem



def test_statistical_vs_history_returns_expected_output():
    invoice_id = uuid4()
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0),
    ]
    line_item_stats = [
        LineItemStats(description="item1", mean_amount=300, stddev_amount=100, n_samples=10),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=10,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=line_item_stats,
        is_degraded=False,
        degradation_reason=None,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_history(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 2
    
    flag_statistical = next(flag for flag in flags if flag.anomaly_name == "line_amount_deviation")
    assert flag_statistical.invoice_id == invoice_id
    assert flag_statistical.anomaly_severity == Severity.red
    assert flag_statistical.anomaly_source == Source.statistical_vs_history
    
    assert flag_statistical.anomaly_notes is not None
    notes_statistical = json.loads(flag_statistical.anomaly_notes)
    
    anomalous_lines = notes_statistical["anomalous_lines"]
    assert len(anomalous_lines) == 1
    
    anomalous_line = anomalous_lines[0]
    assert anomalous_line["description"] == "item1"
    assert anomalous_line["amount_gross"] == 600.0
    assert anomalous_line["historical_mean"] == 300.0
    assert anomalous_line["historical_stddev"] == 100.0
    assert math.isclose(anomalous_line["z_score"], 3.0, rel_tol=1e-4)

    flag_unmatched = next(flag for flag in flags if flag.anomaly_name == "unmatched_line_item")
    assert flag_unmatched.invoice_id == invoice_id
    assert flag_unmatched.anomaly_severity == Severity.yellow
    assert flag_unmatched.anomaly_source == Source.statistical_vs_history
    
    assert flag_unmatched.anomaly_notes is not None
    notes_unmatched = json.loads(flag_unmatched.anomaly_notes)
    
    unmatched_lines = notes_unmatched["unmatched_lines"]
    
    assert len(unmatched_lines) == 1
    assert unmatched_lines[0] == "item2"
    

def test_statistical_vs_history_raises_exception():
        
    state: PipelineState = {
        "invoice_id": uuid4(),
        "invoice_line_items": None,
        "historical_summary": None,
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        statistical_vs_history(state)
        

def test_statistical_vs_history_zscore_none_returns_nothing():
    invoice_id = uuid4()
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
    ]
    line_item_stats = [
        LineItemStats(description="item1", mean_amount=300, stddev_amount=None, n_samples=1),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=1,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=line_item_stats,
        is_degraded=False,
        degradation_reason=None,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_history(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 0
    

def test_statistical_vs_history_returns_statistical_only():
    invoice_id = uuid4()
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
    ]
    line_item_stats = [
        LineItemStats(description="item1", mean_amount=300, stddev_amount=100, n_samples=10),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=10,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=line_item_stats,
        is_degraded=False,
        degradation_reason=None,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_history(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 1
    
    flag_statistical = flags[0]
    assert flag_statistical.anomaly_name == "line_amount_deviation"


def test_statistical_vs_history_returns_unmatched_only():
    invoice_id = uuid4()
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0),
    ]
    line_item_stats = [
        LineItemStats(description="item1", mean_amount=300, stddev_amount=100, n_samples=10),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=10,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=line_item_stats,
        is_degraded=False,
        degradation_reason=None,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_history(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 1

    flag_unmatched = flags[0]
    assert flag_unmatched.anomaly_name == "unmatched_line_item"
    

def test_statistical_vs_history_degraded_history_returns_yellow_severity():
    invoice_id = uuid4()
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0),
    ]
    line_item_stats = [
        LineItemStats(description="item1", mean_amount=300, stddev_amount=100, n_samples=10),
    ]
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=10,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=line_item_stats,
        is_degraded=True,
        degradation_reason=DegradationReason.window_miss,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_history(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 2
    
    flag_statistical = next(flag for flag in flags if flag.anomaly_name == "line_amount_deviation")
    assert flag_statistical.anomaly_severity == Severity.yellow

    flag_unmatched = next(flag for flag in flags if flag.anomaly_name == "unmatched_line_item")
    assert flag_unmatched.anomaly_severity == Severity.yellow
    

def test_statistical_vs_history_degraded_with_no_history_returns_unmatched_only():
    invoice_id = uuid4()
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=400.0),
    ]
    line_item_stats = []
    historical_summary = HistoricalSummary(
        supplier_name="suppl1",
        invoice_count=10,
        fields_seen=set(),
        metadata_keys_seen=set(),
        line_item_stats=line_item_stats,
        is_degraded=True,
        degradation_reason=DegradationReason.no_history,
    )
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_items,
        "historical_summary": historical_summary,
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_history(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 1

    flag_unmatched = flags[0]
    assert flag_unmatched.anomaly_name == "unmatched_line_item"
    
    assert flag_unmatched.anomaly_notes is not None
    notes_unmatched = json.loads(flag_unmatched.anomaly_notes)
    
    unmatched_lines = notes_unmatched["unmatched_lines"]
    
    assert len(unmatched_lines) == 2
    assert "item1" in unmatched_lines
    assert "item2" in unmatched_lines