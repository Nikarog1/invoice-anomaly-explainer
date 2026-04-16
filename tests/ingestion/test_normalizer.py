from uuid import UUID

import pytest

from ingestion.normalizer import Normalizer
from ingestion.models import RawInvoice



def test_normalizer_exact_match_returns_expected_output() -> None:
    
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
    
    normalizer = Normalizer(raw_input)
    invoice, invoice_line_items = normalizer.normalize("./config/columns_mapping.json")
    
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
    

def test_normalizer_zero_input_returns_error() -> None:
    raw_input = []
    
    with pytest.raises(ValueError):
        Normalizer(raw_input)
    
