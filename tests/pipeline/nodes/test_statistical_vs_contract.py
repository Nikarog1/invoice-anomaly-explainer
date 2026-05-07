from datetime import date
from uuid import uuid4

import pytest

from core.exceptions import PipelineStateError

from pipeline.nodes.statistical_vs_contract import statistical_vs_contract
from pipeline.state import PipelineState

from schemas.contract import Contract, ContractLineItem, ContractSummary, ContractWithLineItems
from schemas.invoice import InvoiceLineItem



def _generate_contracts():
    contract_1 = Contract(supplier_name="suppl1", buyer_name="comp", signed_on=date(2026, 1, 1))
    contract_lines_1 = [
        ContractLineItem(contract_id=contract_1.contract_id, product_service_name="item1", unit_price=100.0),
        ContractLineItem(contract_id=contract_1.contract_id, product_service_name="item2", unit_price=500.0),
    ]
    contract_2 = Contract(supplier_name="suppl1", buyer_name="comp", signed_on=date(2026, 1, 1))
    contract_lines_2 = [
        ContractLineItem(contract_id=contract_2.contract_id, product_service_name="item3", unit_price=50.0),
    ]
    
    contract_summary = ContractSummary(
        contracts=[
            ContractWithLineItems(contract=contract_1, line_items=contract_lines_1),
            ContractWithLineItems(contract=contract_2, line_items=contract_lines_2),
        ],
        is_degraded=False,
        degradation_reason=None
    )
    
    return contract_summary 



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
        

def test_statistical_vs_contract_empty_line_match_returns_no_anomaly():
    invoice_id = uuid4()
    invoice_line_item = [
        InvoiceLineItem(invoice_id=invoice_id, description="item1", amount_gross=100.0),
        InvoiceLineItem(invoice_id=invoice_id, description="item2", amount_gross=500.0),
    ]
    contract_summary = _generate_contracts()
    state: PipelineState = {
        "invoice_id": invoice_id,
        "invoice_line_items": invoice_line_item,
        "contract_summary": contract_summary
    } # type: ignore[typeddict-item]
    
    output = statistical_vs_contract(state)
    
    assert len(output["anomaly_flags"]) == 0
    
