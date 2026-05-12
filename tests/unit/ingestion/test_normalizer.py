from uuid import UUID

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.normalizer import Normalizer
from ingestion.models import RawInvoice
from schemas.columns_mapping import ColumnMapping, MappingMethod



def test_normalizer_zero_input_returns_error() -> None:
    raw_input = []
    
    with pytest.raises(ValueError):
        Normalizer(raw_input)
        

async def test_normalizer_exact_match_returns_expected_output() -> None:
    
    raw_input = [
        RawInvoice(**{
            "invoice number": "0123456", 
            "supplier": "Company1", 
            "total": "1234.90",
            "item description": "table",
            "total with tax": "734.90",
            "metadata_field": "metadata_field",
        }),
        RawInvoice(**{
            "invoice number": "0123456", 
            "supplier": "Company1", 
            "total": "1234.90",
            "item description": "chair",
            "total with tax": "500",
            "metadata_field": "metadata_field",
        }),   
    ]
    
    normalizer = Normalizer(raw_input, "./config/columns_mapping.json", confidence_threshold=1.1)
    result = await normalizer.normalize()
    
    invoice = result.invoice
    invoice_line_items = result.invoice_line_items
    
    invoice_id = invoice.invoice_id
    assert isinstance(invoice_id, UUID)
    
    assert invoice.invoice_number == "0123456"
    assert invoice.supplier_name == "Company1"
    assert invoice.total_amount == 1234.90
    assert invoice.invoice_metadata == {"metadata_field": "metadata_field"}
    
    assert len(invoice_line_items) == 2
    
    invoice_line_items_0 = invoice_line_items[0]
    
    assert invoice_line_items_0.description == "table"
    assert invoice_line_items_0.amount_gross == 734.90
    assert isinstance(invoice_line_items_0.invoice_line_item_id, UUID)
    

async def test_normalizer_fuzzy_match_returns_expected_output() -> None:

    raw_input = [
        RawInvoice(**{
            "num invoice": "0123456", 
            "suppl": "Company1", 
            "total Invoice": "1234.90",
            "descr.": "table",
            "Amount gross": "734.90",
            "rate vat %":"15",
            "metadata_field": "metadata_field",
        }),
        RawInvoice(**{
            "num invoice": "0123456", 
            "suppl": "Company1", 
            "total invoice": "1234.90",
            "descr.": "chair",
            "Amount gross": "500",
            "rate vat %":"15",
            "metadata_field": "metadata_field",
        }),   
    ]
    
    normalizer = Normalizer(raw_input, "./config/columns_mapping.json", 0.7)
    result = await normalizer.normalize()
    
    invoice = result.invoice
    invoice_line_items = result.invoice_line_items
    
    invoice_id = invoice.invoice_id
    assert isinstance(invoice_id, UUID)
    
    assert invoice.invoice_number == "0123456"
    assert invoice.supplier_name == "Company1"
    assert invoice.total_amount == 1234.90
    assert invoice.invoice_metadata == {"metadata_field": "metadata_field"}
    
    assert len(invoice_line_items) == 2
    
    invoice_line_items_0 = invoice_line_items[0]
    
    assert invoice_line_items_0.description == "table"
    assert invoice_line_items_0.amount_gross == 734.90
    assert invoice_line_items_0.vat_rate == 15.0
    assert isinstance(invoice_line_items_0.invoice_line_item_id, UUID)
    

async def test_normalizer_llm_match_returns_expected_output() -> None:

    raw_input = [
        RawInvoice(**{
            "num invoice": "0123456", 
            "suppl": "Company1", 
            "total Invoice": "1234.90",
            "descr.": "table",
            "Amount gross": "734.90",
            "rate vat %":"15",
            "metadata_field": "metadata_field",
        }),
        RawInvoice(**{
            "num invoice": "0123456", 
            "suppl": "Company1", 
            "total invoice": "1234.90",
            "descr.": "chair",
            "Amount gross": "500",
            "rate vat %":"15",
            "metadata_field": "metadata_field",
        }),   
    ]
    
    llm_results = [
        ColumnMapping(
            raw_column="num invoice",
            schema_field="invoice_number",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,
        ),
        ColumnMapping(
            raw_column="suppl",
            schema_field="supplier_name",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,
        ),
        ColumnMapping(
            raw_column="total invoice",
            schema_field="total_amount",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,
        ),
        ColumnMapping(
            raw_column="descr.",
            schema_field="description",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,
        ),
        ColumnMapping(
            raw_column="amount gross",
            schema_field="amount_gross",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,
        ),
        ColumnMapping(
            raw_column="rate vat %",
            schema_field="vat_rate",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,
        ),
        ColumnMapping(
            raw_column="metadata_field",
            schema_field=None,
            method=MappingMethod.llm,
            resolved=False,
            confidence=None,
        ),
    ]
    
    normalizer = Normalizer(raw_input, "./config/columns_mapping.json", 1.1)
    
    with patch.object(
        Normalizer, 
        "_llm_match_columns", 
        new=AsyncMock(return_value=llm_results)
    ):
        result = await normalizer.normalize()
    
    invoice = result.invoice
    invoice_line_items = result.invoice_line_items
        
    assert len(invoice_line_items) == 2
    
    invoice_cols = [col for col in invoice.model_dump().keys()]
    invoice_line_item_cols = [col for col in invoice_line_items[0].model_dump().keys()]
    
    assert "invoice_number" in invoice_cols
    assert "supplier_name" in invoice_cols
    assert "total_amount" in invoice_cols
    assert "invoice_metadata" in invoice_cols
    assert "metadata_field" not in invoice_cols
    
    assert "description" in invoice_line_item_cols
    assert "amount_gross" in invoice_line_item_cols
    assert "vat_rate" in invoice_line_item_cols