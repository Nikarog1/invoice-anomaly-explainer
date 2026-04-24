import csv
from uuid import UUID

from sqlmodel import select

from ingestion.repository import IngestionRepository
from ingestion.service import IngestionService
from schemas.columns_mapping import MappingMethod
from schemas.invoice import Invoice, InvoiceLineItem



async def test_ingestion_service_integration(tmp_path, fake_session) -> None:
    data = [
        ["num invoice", "supplier_name", "buyer_name", "total_amount", "description", "amount_gross"],
        ["012345", "suppl1", "SuperCompany", "1000.0", "item1", "700.0"],
        ["012345", "suppl1", "SuperCompany", "1000.0", "item2", "200.0"],
        ["012345", "suppl1", "SuperCompany", "1000.0", "item3", "100.0"],
    ]
    
    path = tmp_path / "data.csv"
    with open(path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
        
    invoice_id = await IngestionService().run(path_to_csv=path, confidence_threshold=0.7)
    
    assert isinstance(invoice_id, UUID)
    
    invoice = fake_session.exec(select(Invoice)).first()
    invoice_line_items = fake_session.exec(select(InvoiceLineItem)).all()
    
    assert invoice.invoice_id == invoice_id
    assert invoice.invoice_number == "012345"
    
    assert len(invoice_line_items) == 3
    
    mapping = IngestionRepository().load_mappings(invoice_id=invoice_id)
    assert len(mapping) == 6
    
    mapping_invoice_number = next(m for m in mapping if m.raw_column == "num invoice")
    assert mapping_invoice_number.method == MappingMethod.fuzzy