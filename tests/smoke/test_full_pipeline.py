import csv
from datetime import date
from pathlib import Path
from uuid import UUID

import pytest

from data.sqlite import get_session
from data.vector_store import add_contract_line_items, get_collection

from ingestion.service import IngestionService
from pipeline.service import run_pipeline

from schemas.contract import Contract, ContractLineItem, ContractWithLineItems


def _generate_invoices(
        tmp_path: Path,
        item_1: list[str],
        item_2: list[str],
        iteration: int,
    ) -> Path:
    data = [
        [
            "invoice_number", "supplier_name", "issue_date", "buyer_name", "total_amount", "currency",
            "description", "quantity", "unit_price", "vat_rate", "amount_gross", "metadata_col",
        ],
        item_1,
        item_2,
    ]
    
    path = tmp_path / f"data_{iteration}.csv"
    with open(path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
        
    return path



def _generate_contracts() -> None:
    contract = Contract(supplier_name="suppl1", buyer_name="our_company", signed_on=date(2025, 7, 1))
    contract_line_items = [
        ContractLineItem(contract_id=contract.contract_id, product_service_name="table", unit_price=416.66, max_units=1.0),
        ContractLineItem(contract_id=contract.contract_id, product_service_name="chair", unit_price=416.66, max_units=1.0),
    ]
    
    with get_session() as session:
        session.add(contract)
        session.add_all(contract_line_items)
        session.commit()

    contract_with_items = ContractWithLineItems(
        contract=contract,
        line_items=contract_line_items,
    )
    
    collection = get_collection()
    add_contract_line_items(collection, contract_with_items)     
    
        

@pytest.mark.smoke
async def test_full_pipeline(tmp_path: Path) -> None:
    items_1 = [
        ["012344", "suppl1", "2025-08-31", "our_company", "1000.0", "CZK", "table", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012345", "suppl1", "2025-09-30", "our_company", "1000.0", "CZK", "table", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012346", "suppl1", "2025-10-31", "our_company", "1000.0", "CZK", "table", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012347", "suppl1", "2025-11-30", "our_company", "1000.0", "CZK", "table", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012348", "suppl1", "2025-12-31", "our_company", "2000.0", "CZK", "table", "1.0", "833.33", "20.0", "1000.0", "meta_field"],
    ]
    items_2 = [
        ["012344", "suppl1", "2025-08-31", "our_company", "1000.0", "CZK", "chair", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012345", "suppl1", "2025-09-30", "our_company", "1000.0", "CZK", "chair", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012346", "suppl1", "2025-10-31", "our_company", "1000.0", "CZK", "chair", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012347", "suppl1", "2025-11-30", "our_company", "1000.0", "CZK", "chair", "1.0", "416.66", "20.0", "500.0", "meta_field"],
        ["012348", "suppl1", "2025-12-31", "our_company", "2000.0", "CZK", "chair", "2.0", "416.66", "20.0", "1000.0", "meta_field"],
    ]

    invoice_id = None
    ingestion = IngestionService()
    
    for i, (item1, item2) in enumerate(zip(items_1, items_2)):
        path = _generate_invoices(tmp_path, item1, item2, i)
        invoice_id = await ingestion.run(path_to_csv=path) 
    assert invoice_id
    assert isinstance(invoice_id, UUID)
    
    _generate_contracts()

    report = await run_pipeline(invoice_id)
    
    assert report
    assert report.invoice_id == invoice_id
    assert report.anomalies_count >= 1
    assert report.agent_explanation
    assert report.explanation_date