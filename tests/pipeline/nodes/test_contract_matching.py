from datetime import date
import json

from sqlmodel import select

import pytest

from core.exceptions import PipelineRepositoryError, PipelineStateError

from pipeline.nodes.contract_matching import contract_matching
from pipeline.state import PipelineState

from schemas.anomaly import Severity, Source
from schemas.contract import Contract, ContractLineItem, ContractSummary, ContractWithLineItems, DegradationReason
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.junction import LineItemMatch, Method



def _generate_invoice(line_desc: list = ["item1", "item2"]):
    invoice = Invoice(
        invoice_number="12345",
        supplier_name="suppl1",
        total_amount=1000.0
    )
    invoice_id = invoice.invoice_id
    inv_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description=line_desc[0], amount_gross=400.0),
        InvoiceLineItem(invoice_id=invoice_id, description=line_desc[1], amount_gross=600.0),
    ]
    return (invoice_id, invoice, inv_line_items)

def _generate_contract(line_desc: list = ["item1", "item2"]):
    contract = Contract(supplier_name="suppl1", buyer_name="our_comp", signed_on=date(2026, 1, 1))
    contract_id = contract.contract_id
    con_line_items = [
        ContractLineItem(contract_id=contract_id, product_service_name=line_desc[0], unit_price=400.0),
        ContractLineItem(contract_id=contract_id, product_service_name=line_desc[1], unit_price=600.0),
    ]
    contract_summary = ContractSummary(
        contracts=[ContractWithLineItems(contract=contract, line_items=con_line_items)],
        is_degraded=False,
        degradation_reason=None
    )
    return (contract_summary, con_line_items)
    
    
async def test_contract_matching_exact_match_writes_to_table_and_returns_no_anomaly_flag(fake_session):
    invoice_id, invoice, inv_line_items = _generate_invoice()
    contract_summary, con_line_items = _generate_contract()
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": inv_line_items,
        "contract_summary": contract_summary,
    } # type: ignore[typeddict-item]
    
    output = await contract_matching(state)
    
    assert len(output["anomaly_flags"]) == 0
    
    result = fake_session.exec(
        select(LineItemMatch)
    ).all()
    assert len(result) == 2
    
    inv_line1 = next(i for i in inv_line_items if i.description == "item1")
    con_line1 = next(c for c in con_line_items if c.product_service_name == "item1")
    result_item1 = next(r for r in result if r.invoice_line_item_id == inv_line1.invoice_line_item_id)
    
    assert result_item1.invoice_line_item_id == inv_line1.invoice_line_item_id
    assert result_item1.contract_line_item_id == con_line1.contract_line_item_id
    assert result_item1.match_method == Method.exact
    assert result_item1.match_score == 1.0
    

async def test_contract_matching_raises_pipeline_state_error():
    state: PipelineState = {
        "invoice": None,
        "invoice_line_items": None,
        "contract_summary": None
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        await contract_matching(state)
        

async def test_contract_matching_degraded_returns_no_anomalies_no_db_write(fake_session):
    invoice_id, invoice, inv_line_items = _generate_invoice()
    
    contract_summary = ContractSummary(
        contracts=[],
        is_degraded=True,
        degradation_reason=DegradationReason.no_contract
    )
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": inv_line_items,
        "contract_summary": contract_summary,
    } # type: ignore[typeddict-item]
    
    output = await contract_matching(state)
    
    assert len(output["anomaly_flags"]) == 0
    
    result = fake_session.exec(
        select(LineItemMatch)
    ).all()
    assert len(result) == 0
    

async def test_contract_matching_already_mapped_no_anomaly(fake_session):
    invoice_id, invoice, inv_line_items = _generate_invoice()
    contract_summary, con_line_items = _generate_contract()
    
    mapping = [
        LineItemMatch(
            contract_line_item_id=next(line.contract_line_item_id for line in con_line_items if line.product_service_name == "item1"),
            invoice_line_item_id=next(line.invoice_line_item_id for line in inv_line_items if line.description == "item1"),
            match_method=Method.exact,
            match_score=1.0,
        ),
        LineItemMatch(
            contract_line_item_id=next(line.contract_line_item_id for line in con_line_items if line.product_service_name == "item2"),
            invoice_line_item_id=next(line.invoice_line_item_id for line in inv_line_items if line.description == "item2"),
            match_method=Method.exact,
            match_score=1.0,
        ),
    ]
    
    fake_session.add_all(mapping)
    fake_session.commit()
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": inv_line_items,
        "contract_summary": contract_summary,
    } # type: ignore[typeddict-item]
    
    output = await contract_matching(state)
    
    assert len(output["anomaly_flags"]) == 0
    
    result = fake_session.exec(
        select(LineItemMatch)
    ).all()
    assert list(result) == mapping
    

async def test_contract_matching_fuzzy_match_writes_to_table_and_returns_expected_output(fake_session):
    invoice_id, invoice, inv_line_items = _generate_invoice(line_desc=["cleaning service apr", "cleaning materials"])
    contract_summary, con_line_items = _generate_contract(line_desc=["cleaning services", "cleaning material"])
    
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice": invoice,
        "invoice_line_items": inv_line_items,
        "contract_summary": contract_summary,
    } # type: ignore[typeddict-item]
    
    output = await contract_matching(state)
    
    assert len(output["anomaly_flags"]) == 1
    
    flag = output["anomaly_flags"][0]
    assert flag.invoice_id == invoice_id
    assert flag.anomaly_name=="not_exact_match"
    assert flag.anomaly_severity==Severity.yellow
    assert flag.anomaly_source==Source.contract_matching
    assert flag.anomaly_deviation is None
    
    assert flag.anomaly_notes is not None
    notes = json.loads(flag.anomaly_notes)
    assert len(notes["fuzzy_resolved"]) == 2
    
    fuzzy_notes = notes["fuzzy_resolved"]
    notes_0 = next(n for n in fuzzy_notes if n["invoice_description"] == "cleaning service apr")
    assert notes_0["invoice_description"] == "cleaning service apr"
    assert notes_0["matched_contract_name"] == "cleaning services"
    
    result = fake_session.exec(
        select(LineItemMatch)
    ).all()
    assert len(result) == 2
    
    inv_line1 = next(i for i in inv_line_items if i.description == "cleaning service apr")
    con_line1 = next(c for c in con_line_items if c.product_service_name == "cleaning services")
    result_item1 = next(r for r in result if r.invoice_line_item_id == inv_line1.invoice_line_item_id)
    
    assert result_item1.invoice_line_item_id == inv_line1.invoice_line_item_id
    assert result_item1.contract_line_item_id == con_line1.contract_line_item_id
    assert result_item1.match_method == Method.fuzzy
    
