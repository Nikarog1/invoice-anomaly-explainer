from uuid import UUID

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.normalizer import Normalizer
from ingestion.models import RawInvoice
from schemas.columns_mapping import ColumnMappingResult



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
    invoice, invoice_line_items = await normalizer.normalize()
    
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
    invoice, invoice_line_items = await normalizer.normalize()
    
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
        ColumnMappingResult(
            raw_column="num invoice",
            schema_field="invoice_number",
            method="llm",
            resolved=True,
            confidence=0.6,
        ),
        ColumnMappingResult(
            raw_column="suppl",
            schema_field="supplier_name",
            method="llm",
            resolved=True,
            confidence=0.6,
        ),
        ColumnMappingResult(
            raw_column="total invoice",
            schema_field="total_amount",
            method="llm",
            resolved=True,
            confidence=0.6,
        ),
        ColumnMappingResult(
            raw_column="descr.",
            schema_field="description",
            method="llm",
            resolved=True,
            confidence=0.6,
        ),
        ColumnMappingResult(
            raw_column="amount gross",
            schema_field="amount_gross",
            method="llm",
            resolved=True,
            confidence=0.6,
        ),
        ColumnMappingResult(
            raw_column="rate vat %",
            schema_field="vat_rate",
            method="llm",
            resolved=True,
            confidence=0.6,
        ),
        ColumnMappingResult(
            raw_column="metadata_field",
            schema_field=None,
            method="llm",
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
        invoice, invoice_line_items = await normalizer.normalize()
        
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


async def test_normalizer_llm_match_hhtpx_client_returns_expected_output() -> None:
    response = {
        "message": {
            "content": 
                '{"num invoice": "invoice_number", '
                    +'"suppl":"supplier_name", '
                    +'"total invoice":"total_amount", '
                    +'"metadata_field": null, '
                    +'"hallucination": "happened"}'
        }
    }
    unresolved_schema_fields = ["invoice_number", "supplier_name", "total_amount"]
    unresolved_raw_fields = ["num invoice", "suppl", "total invoice", "metadata_field", "hallucination"]
    
    mock_response = MagicMock()
    mock_response.json.return_value = response
    
    mock_client = AsyncMock()
    mock_client.send.return_value = mock_response
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__ = AsyncMock(return_value=None)
    
    mapping = Normalizer._read_columns_mapping_json("./config/columns_mapping.json")
    
    with patch("ingestion.normalizer.AsyncClient", return_value=mock_client):
        result = await Normalizer._llm_match_columns(
            "some_url",
            "some_model",
            "some_prompt",
            mapping,
            unresolved_schema_fields,
            unresolved_raw_fields
        )
    assert len(result) == 5
    
    assert result[0].raw_column == "num invoice"
    assert result[0].schema_field == "invoice_number"
    assert result[0].method == "llm"
    assert result[0].resolved == True
    assert result[0].confidence == 0.6
    
    assert result[-1].schema_field == None
    assert result[-1].resolved == False
    
    assert result[-2].resolved == False