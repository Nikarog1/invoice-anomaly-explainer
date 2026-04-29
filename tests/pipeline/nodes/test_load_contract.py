from datetime import date

import pytest

from core.exceptions import PipelineStateError
from pipeline.state import PipelineState
from pipeline.nodes.load_contract import load_contract
from schemas.contract import Contract, ContractLineItem, DegradationReason
from schemas.invoice import Invoice



def _generate_state(issue_date: date | None = date(2026, 3, 22)):
    invoice = Invoice(
        invoice_number="12345",
        supplier_name="suppl1",
        total_amount=2000.0,
        issue_date=issue_date
    )
    
    state: PipelineState = {
        "invoice": invoice
    } # type: ignore[typeddict-item]
    
    return state
    
def test_load_contract_return_expected_output(fake_session):
    contracts = [
        Contract(supplier_name="suppl1", buyer_name="our_company", signed_on=date(2026, 1, 1)),
        Contract(supplier_name="suppl1", buyer_name="our_company", signed_on=date(2026, 2, 1)),
    ]
    contract_line_items = [
        ContractLineItem(contract_id=contracts[0].contract_id, product_service_name="item1", unit_price=100.0),
        ContractLineItem(contract_id=contracts[0].contract_id, product_service_name="item2", unit_price=50.0),
        ContractLineItem(contract_id=contracts[1].contract_id, product_service_name="item3", unit_price=1000.0),
        ContractLineItem(contract_id=contracts[1].contract_id, product_service_name="item4", unit_price=750.0),
    ]
    
    fake_session.add_all(contracts)
    fake_session.add_all(contract_line_items)
    fake_session.commit()
    
    state = _generate_state()
    
    output = load_contract(state)
    contract_summary = output["contract_summary"]
    
    assert contract_summary
    assert not contract_summary.is_degraded
    assert contract_summary.degradation_reason is None
    
    results = contract_summary.contracts
    assert len(results) == 2
    
    contract_1 = next(con for con in results if con.contract.signed_on == date(2026, 1, 1))
    assert contract_1.contract.supplier_name == "suppl1"
    assert contract_1.contract.buyer_name == "our_company"
    assert contract_1.contract.signed_on == date(2026, 1, 1)
    
    assert len(contract_1.line_items) == 2
    contract_1_id = contract_1.contract.contract_id
    
    contract_1_line_1 = next(line for line in contract_1.line_items if line.unit_price == 100.0)
    assert contract_1_line_1.contract_id == contract_1_id
    assert contract_1_line_1.product_service_name == "item1"
    assert contract_1_line_1.unit_price == 100.0
    

def test_load_contract_raises_exception():
    invoice = None
    
    state: PipelineState = {
        "invoice": invoice
    } # type: ignore[typeddict-item]
    
    with pytest.raises(PipelineStateError):
        load_contract(state)
        

def test_load_contract_missing_issue_date_return_empty():
    state = _generate_state(issue_date=None)
    
    output = load_contract(state)
    contract_summary = output["contract_summary"]
    
    assert len(contract_summary.contracts) == 0
    assert contract_summary.is_degraded
    assert contract_summary.degradation_reason == DegradationReason.issue_date_missing
    

def test_load_contract_different_supplier_return_empty(fake_session):
    state = _generate_state()
    
    contract = Contract(supplier_name="suppl2", buyer_name="our_company", signed_on=date(2026, 1, 1))
    contract_line_items = [
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item1", unit_price=100.0),
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item2", unit_price=50.0),
    ]
    
    fake_session.add(contract)
    fake_session.add_all(contract_line_items)
    fake_session.commit()
    
    state = _generate_state()
    
    output = load_contract(state)
    contract_summary = output["contract_summary"]
    
    assert len(contract_summary.contracts) == 0
    assert contract_summary.is_degraded
    assert contract_summary.degradation_reason == DegradationReason.no_contract
    

def test_load_contract_signed_on_after_issue_date_return_empty(fake_session):
    state = _generate_state()
    
    contract = Contract(supplier_name="suppl1", buyer_name="our_company", signed_on=date(2026, 4, 1))
    contract_line_items = [
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item1", unit_price=100.0),
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item2", unit_price=50.0),
    ]
    
    fake_session.add(contract)
    fake_session.add_all(contract_line_items)
    fake_session.commit()
    
    state = _generate_state()
    
    output = load_contract(state)
    contract_summary = output["contract_summary"]
    
    assert len(contract_summary.contracts) == 0
    assert contract_summary.is_degraded
    assert contract_summary.degradation_reason == DegradationReason.no_contract
    

def test_load_contract_expires_on_before_issue_date_return_empty(fake_session):
    state = _generate_state()
    
    contract = Contract(supplier_name="suppl1", buyer_name="our_company", signed_on=date(2026, 1, 1), expires_on=date(2026, 2, 1))
    contract_line_items = [
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item1", unit_price=100.0),
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item2", unit_price=50.0),
    ]
    
    fake_session.add(contract)
    fake_session.add_all(contract_line_items)
    fake_session.commit()
    
    state = _generate_state()
    
    output = load_contract(state)
    contract_summary = output["contract_summary"]
    
    assert len(contract_summary.contracts) == 0
    assert contract_summary.is_degraded
    assert contract_summary.degradation_reason == DegradationReason.no_contract
    

def test_load_contract_signed_on_equals_issue_date_return_expected(fake_session):
    state = _generate_state()
    
    contract = Contract(supplier_name="suppl1", buyer_name="our_company", signed_on=date(2026, 3, 22))
    contract_line_items = [
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item1", unit_price=100.0),
        ContractLineItem(contract_id=contract.contract_id, product_service_name="item2", unit_price=50.0),
    ]
    
    fake_session.add(contract)
    fake_session.add_all(contract_line_items)
    fake_session.commit()
    
    state = _generate_state()
    
    output = load_contract(state)
    contract_summary = output["contract_summary"]
    
    assert len(contract_summary.contracts) == 1
    assert not contract_summary.is_degraded
    assert contract_summary.degradation_reason is None