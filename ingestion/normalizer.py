from __future__ import annotations 

import json

from pathlib import Path
from typing import Any
from uuid import UUID

from ingestion.models import RawInvoice
from schemas.invoice import Invoice, InvoiceLineItem



class Normalizer:
    """
    Normalizes raw csv into definied pydantic models.
    
    Args:
        data: list of RawInvoice object from CSVParser
    """
    
    def __init__(self, data: list[RawInvoice]) -> None:
        if len(data) == 0:
            raise ValueError("Provided data must contain at least 1 row!")
        self._data = data
        
    def normalize(self, path: str|Path) -> tuple[Invoice, list[InvoiceLineItem]]:
        """
        Runs full normalization pipeline.
        Loads columns_mapping, maps raw columns to it, applies it to all raw data rows.
        Then, creates Pydantic (SQLModel) objects - Invoice and list with InvoiceLineItem.
        
        Args:
            path: path to columns_mapping
        
        Returns:
            Tuple of pydantic objects - Invoice and list of InvoiceLineItem
        """
        
        columns_mapping = self._read_columns_mapping_json(path)
        
        single_row = self._data[0]
        raw_columns = list(single_row.model_dump().keys())
        
        mapped_columns = self._map_columns(raw_columns, columns_mapping)
        mapped_data = self._apply_mapping(mapped_columns)
        
        invoice = self._build_invoice(mapped_data[0])
        invoice_id = invoice.invoice_id

        invoice_line_items = self._build_line_items(mapped_data, invoice_id)
        
        return (invoice, invoice_line_items)
    
    
    def _build_invoice(self, mapped_row: dict[str, Any]) -> Invoice:
        """
        Builds Invoice object.
        
        Args:
            mapped_data: raw data mapped to desired column names from _apply_mapping
        
        Returns:
            Invoice object
        """
        invoice_cols = Invoice.model_fields.keys()
        
        mapped_row_invoice = {key: value for key, value in mapped_row.items() if key in invoice_cols}
        
        return Invoice.model_validate(mapped_row_invoice)

        
    def _build_line_items(self, mapped_data: list[dict[str, Any]], invoice_id: UUID) -> list[InvoiceLineItem]:
        """
        Builds InvoiceLineItem objects.
        
        Args:
            mapped_data: list of raw data mapped to desired column names from _apply_mapping
            invoice_id: uuid generated in Invoice object (foreign key here)
        
        Returns:
            List of InvoiceLineItem objects
        """
        line_items_cols = InvoiceLineItem.model_fields.keys()
        
        results = []
        for data in mapped_data:
            mapped_data_line_item = {key: value for key, value in data.items() if key in line_items_cols}
            mapped_data_line_item["invoice_id"] = invoice_id

            results.append(InvoiceLineItem.model_validate(mapped_data_line_item))
            
        return results


    def _apply_mapping(self, mapped_cols: dict[str, str]) -> list[dict[str, Any]]:
        """
        Applies mapping from _map_columns to all data.
        
        Args:
            mapped_cols: result from _map_columns
        
        Returns:
            List of dicts with row data mapped to desired column names
        """
        results = []
        
        for row in self._data:
            row_dict = {}
            
            for col, value in row.model_dump().items():
                mapped_col = mapped_cols.get(col)
                
                if mapped_col is None:
                    if "invoice_metadata" not in row_dict:
                        row_dict["invoice_metadata"] = {}
                        
                    row_dict["invoice_metadata"][col] = value
                else:
                    row_dict[mapped_col] = value
                    
            results.append(row_dict)

        return results          


    @staticmethod
    def _map_columns(raw_columns: list, mapping: dict) -> dict[str, str]:
        """
        Maps columns from RawInvoice to Pydantic models using columns_mapping.
        
        Args:
            raw_columns: columns from RawInvoice
            mapping: columns_mapping
        
        Returns:
            Dict with cols from RawInvoice (key) mapped to desired output (values)
        """
        results = {}
        raw_columns_lower = [col.lower() for col in raw_columns]
        
        for key, values in mapping.items():
            for raw_col in raw_columns_lower:
                if raw_col in results and raw_col in values:
                    raise ValueError(f"Column '{raw_col}' maps to both '{results[raw_col]}' and '{key}'")
                if raw_col in values:
                    results[raw_col] = key
                    
        return results
    
    @staticmethod
    def _read_columns_mapping_json(path: str|Path) -> dict[str, list]:
        """
        Loads columns_mapping for exact string comparison and mapping. 
        From raw provided cols to defined in pydantic.
        
        Args:
            path: path to columns_mapping
            
        Returns:
            Dict with desired cols (keys) and possible variants from RawInvoice as lists (values)
        """
        path_converted = Path(path)
        
        if not path_converted.is_file():
            raise FileNotFoundError(f"There is no file in provided path: {path_converted}") 
        
        with open(path_converted) as f:
            return json.load(f)
        
                    
            

