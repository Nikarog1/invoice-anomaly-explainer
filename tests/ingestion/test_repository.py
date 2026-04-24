from uuid import uuid4
from sqlmodel import select

import pytest

from core.exceptions import InvoiceMappingNotFoundError
from ingestion.models import IngestionResult
from ingestion.repository import IngestionRepository
from schemas.columns_mapping import ColumnMapping, ColumnMappingResult, MappingMethod
from schemas.invoice import Invoice, InvoiceLineItem



def _generate_ingestion_result() -> IngestionResult:
    invoice = Invoice(
        invoice_number="num1",
        supplier_name="suppl1",
        buyer_name="our_company",
        total_amount=1000.0
    )
    invoice_line_items = [
        InvoiceLineItem(invoice_id=invoice.invoice_id, description="item1", amount_gross=600.0),
        InvoiceLineItem(invoice_id=invoice.invoice_id, description="item2", amount_gross=400.0),
    ]
    column_mapping_results = [
        ColumnMapping(raw_column="invoice_number", schema_field="invoice_number", method=MappingMethod.exact, resolved=True, confidence=None),
        ColumnMapping(raw_column="supplier_name", schema_field="supplier_name", method=MappingMethod.exact, resolved=True, confidence=None),
        ColumnMapping(raw_column="buyer_name", schema_field="buyer_name", method=MappingMethod.exact, resolved=True, confidence=None),
        ColumnMapping(raw_column="amount total", schema_field="total_amount", method=MappingMethod.fuzzy, resolved=True, confidence=0.95),
        ColumnMapping(raw_column="dDddEsssCriiiption", schema_field="description", method=MappingMethod.llm, resolved=True, confidence=0.6),
        ColumnMapping(raw_column="amount_gross", schema_field="amount_gross", method=MappingMethod.exact, resolved=True, confidence=0.6),
    ]
    
    return IngestionResult(invoice=invoice, invoice_line_items=invoice_line_items, column_mapping_results=column_mapping_results)


def test_ingestion_repository_successful_save(fake_session) -> None:
    ingestion_result = _generate_ingestion_result()
    invoice_id = ingestion_result.invoice.invoice_id
    IngestionRepository().save(ingestion_result)
    
    loaded_invoice = fake_session.exec(select(Invoice)).first()
    loaded_invoice_items = fake_session.exec(select(InvoiceLineItem)).all()
    loaded_column_mapping = fake_session.exec(select(ColumnMappingResult)).all()
    
    assert loaded_invoice is not None
    assert loaded_invoice.invoice_id == invoice_id
    
    assert len(loaded_invoice_items) == 2
    assert len(loaded_column_mapping) == 6
    

def test_ingestion_repository_column_mapping_round_trip(fake_session) -> None: 
    ingestion_result = _generate_ingestion_result()
    invoice_id = ingestion_result.invoice.invoice_id
    
    repo = IngestionRepository()
    repo.save(ingestion_result)
    column_mapping_results = repo.load_mappings(invoice_id)
    
    assert column_mapping_results
    assert len(column_mapping_results) == 6
    
    column_mapping_results_0 = next(cm for cm in column_mapping_results if cm.raw_column == "invoice_number")
    assert isinstance(column_mapping_results_0, ColumnMappingResult)
    assert column_mapping_results_0.invoice_id == invoice_id
    assert column_mapping_results_0.raw_column == "invoice_number"
    assert column_mapping_results_0.schema_field == "invoice_number"
    assert column_mapping_results_0.method == MappingMethod.exact
    assert column_mapping_results_0.resolved is True
    assert column_mapping_results_0.confidence is None
    

def test_ingestion_repository_raises_invoice_column_mapping_error(fake_session) -> None: 
    with pytest.raises(InvoiceMappingNotFoundError):
        IngestionRepository().load_mappings(uuid4())