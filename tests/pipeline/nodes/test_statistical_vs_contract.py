from datetime import date
import json
import math
from uuid import uuid4, UUID

import pytest

from core.exceptions import PipelineStateError

from pipeline.nodes.statistical_vs_contract import statistical_vs_contract
from pipeline.state import PipelineState

from schemas.anomaly import Severity, Source
from schemas.contract import Contract, ContractLineItem, ContractSummary, ContractWithLineItems
from schemas.junction import LineItemMatch, Method
from schemas.invoice import InvoiceLineItem



def _generate_contracts(return_ids: bool = False):
    contract_1 = Contract(supplier_name="suppl1", buyer_name="comp", signed_on=date(2026, 1, 1))
    contract_lines_1 = [
        ContractLineItem(contract_id=contract_1.contract_id, product_service_name="item1", unit_price=100.0, max_units=1),
        ContractLineItem(contract_id=contract_1.contract_id, product_service_name="item2", unit_price=500.0, max_units=1),
    ]
    contract_2 = Contract(supplier_name="suppl1", buyer_name="comp", signed_on=date(2026, 1, 1))
    contract_lines_2 = [
        ContractLineItem(contract_id=contract_2.contract_id, product_service_name="item3", unit_price=50.0, max_units=1),
    ]
    
    contract_summary = ContractSummary(
        contracts=[
            ContractWithLineItems(contract=contract_1, line_items=contract_lines_1),
            ContractWithLineItems(contract=contract_2, line_items=contract_lines_2),
        ],
        is_degraded=False,
        degradation_reason=None
    )
    
    if return_ids:
        return (
            contract_summary, 
            [line.contract_line_item_id for line in [*contract_lines_1, *contract_lines_2]]
        )
    else:
        return contract_summary 
    

def _insert_line_match(fake_session, invoice_line_ids: list[UUID], contract_line_ids: list[UUID]) -> None:
    to_insert = []
    for inv_line_id, con_line_id in zip(invoice_line_ids, contract_line_ids):
        to_insert.append(
            LineItemMatch(
                contract_line_item_id=con_line_id,
                invoice_line_item_id=inv_line_id,
                match_method=Method.exact,
                match_score=1.0
            )
        )
    fake_session.add_all(to_insert)
    fake_session.commit()
    



def test_statistical_vs_contract_raises_pipeline_exception():
    invoice_id = uuid4()
    invoice_line_item = None
    contract_summary = None
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        statistical_vs_contract(state)
        

def test_statistical_vs_contract_degraded_contract_returns_no_anomaly():
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0, unit_price=100.0, quantity=100),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0, unit_price=500.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item3", amount_gross=500.0, unit_price=500.0, quantity=1),
    ]
    contract_summary = _generate_contracts(return_ids=False)
    contract_summary.is_degraded = True # type: ignore
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary # type: ignore
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    
    assert len(output["anomaly_flags"]) == 0
    

def test_statistical_vs_contract_empty_line_match_returns_no_anomaly(fake_session):
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0, unit_price=100.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0, unit_price=500.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item3", amount_gross=500.0, unit_price=500.0, quantity=1),
    ]
    contract_summary = _generate_contracts(return_ids=False)
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary # type: ignore
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    
    assert len(output["anomaly_flags"]) == 0
    

def test_statistical_vs_contract_returns_anomalous_price_flag(fake_session):
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0, unit_price=100.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0, unit_price=500.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item3", amount_gross=500.0, unit_price=500.0, quantity=1),
    ]
    contract_summary, con_line_ids = _generate_contracts(return_ids=True)
    inv_line_ids = [line.invoice_line_item_id for line in invoice_line_item]
    
    _insert_line_match(fake_session, inv_line_ids, con_line_ids) # type: ignore
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary # type: ignore
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 1
    
    flag = flags[0]
    assert flag.invoice_id == invoice_id
    assert flag.anomaly_name == "unit_price_deviation"
    assert flag.anomaly_severity == Severity.red
    assert flag.anomaly_source == Source.statistical_vs_contract
    assert flag.anomaly_deviation is None
    
    assert flag.anomaly_notes is not None
    notes = json.loads(flag.anomaly_notes)
    
    lines = notes["anomalous_lines"]
    
    assert len(lines) == 1
    
    line = lines[0]
    
    assert line["description"] == "item3"
    assert line["invoice"] == 500.0
    assert line["contract"] == 50.0
    assert math.isclose(line["deviation"], (500.0 - 50.0) / 50.0, rel_tol=1e-4)
    assert line["metric"] == "unit_price"
    

def test_statistical_vs_contract_returns_anomalous_quantity_flag(fake_session):
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0, unit_price=100.0, quantity=10),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0, unit_price=500.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item3", amount_gross=50.0, unit_price=50.0, quantity=1),
    ]
    contract_summary, con_line_ids = _generate_contracts(return_ids=True)
    inv_line_ids = [line.invoice_line_item_id for line in invoice_line_item]
    
    _insert_line_match(fake_session, inv_line_ids, con_line_ids) # type: ignore
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary # type: ignore
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 1
    
    flag = flags[0]
    assert flag.invoice_id == invoice_id
    assert flag.anomaly_name == "quantity_deviation"
    assert flag.anomaly_severity == Severity.red
    assert flag.anomaly_source == Source.statistical_vs_contract
    assert flag.anomaly_deviation is None
    
    assert flag.anomaly_notes is not None
    notes = json.loads(flag.anomaly_notes)
    
    lines = notes["anomalous_lines"]
    
    assert len(lines) == 1
    
    line = lines[0]
    
    assert line["description"] == "item1"
    assert line["invoice"] == 10.0
    assert line["contract"] == 1.0
    assert math.isclose(line["deviation"], (10.0 - 1.0) / 1.0, rel_tol=1e-4)
    assert line["metric"] == "quantity"
    

def test_statistical_vs_contract_returns_anomalous_missing_field_flag(fake_session):
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0, unit_price=100.0, quantity=None),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0, unit_price=500.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item3", amount_gross=50.0, unit_price=50.0, quantity=1),
    ]
    contract_summary, con_line_ids = _generate_contracts(return_ids=True)
    inv_line_ids = [line.invoice_line_item_id for line in invoice_line_item]
    
    _insert_line_match(fake_session, inv_line_ids, con_line_ids) # type: ignore
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary # type: ignore
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 1
    
    flag = flags[0]
    assert flag.invoice_id == invoice_id
    assert flag.anomaly_name == "missing_fields"
    assert flag.anomaly_severity == Severity.yellow
    assert flag.anomaly_source == Source.statistical_vs_contract
    assert flag.anomaly_deviation is None
    
    assert flag.anomaly_notes is not None
    notes = json.loads(flag.anomaly_notes)
    
    lines = notes["lines_with_missing_fields"]
    
    assert len(lines) == 1
    
    line = lines[0]
    
    assert line["description"] == "item1"
    
    missing_fields = line["missing_fields"]
    missing_field = missing_fields[0]
    assert missing_field["field"] == "quantity"
    assert missing_field["side"] == "invoice"
    

def test_statistical_vs_contract_returns_all_flags(fake_session):
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0, unit_price=100.0, quantity=None),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0, unit_price=500000.0, quantity=1),
        InvoiceLineItem(invoice_id=invoice_id, description="item3", amount_gross=50.0, unit_price=50.0, quantity=10000),
    ]
    contract_summary, con_line_ids = _generate_contracts(return_ids=True)
    inv_line_ids = [line.invoice_line_item_id for line in invoice_line_item]
    
    _insert_line_match(fake_session, inv_line_ids, con_line_ids) # type: ignore
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary # type: ignore
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    flags = output["anomaly_flags"]
    
    assert len(flags) == 3
    
    names = {f.anomaly_name for f in flags}
    assert names == {"unit_price_deviation", "quantity_deviation", "missing_fields"}
    
    

    
    
