from datetime import date

from sqlmodel import select

import pytest

from core.exceptions import PipelineRepositoryError, PipelineStateError

from pipeline.nodes.contract_matching import contract_matching
from pipeline.state import PipelineState

from schemas.contract import Contract, ContractLineItem, ContractSummary, ContractWithLineItems, DegradationReason
from schemas.invoice import Invoice, InvoiceLineItem
from schemas.junction import LineItemMatch, Method



async def test_contract_matching_exact_match_only_writes_to_table_and_returns_no_anomaly_flag(fake_session):
    invoice = Invoice(
        invoice_number="12345",
        supplier_name="suppl1",
        total_amount=1000.0
    )
    invoice_id = invoice.invoice_id
    inv_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=400.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=600.0),
    ]
    
    contract = Contract(supplier_name="suppl1", buyer_name="our_comp", signed_on=date(2026, 1, 1))
    contract_id = contract.contract_id
    con_line_items = [
        ContractLineItem(contract_id=contract_id, product_service_name="item1", unit_price=400.0),
        ContractLineItem(contract_id=contract_id, product_service_name="item2", unit_price=600.0),
    ]
    contract_summary = ContractSummary(
        contracts=[ContractWithLineItems(contract=contract, line_items=con_line_items)],
        is_degraded=False,
        degradation_reason=None
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
        

async def test_contract_degraded_contract_summary_returns_no_anomalies_and_dont_write_to_db(fake_session):
    invoice = Invoice(
        invoice_number="12345",
        supplier_name="suppl1",
        total_amount=1000.0
    )
    invoice_id = invoice.invoice_id
    inv_line_items = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=400.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=600.0),
    ]
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
    
