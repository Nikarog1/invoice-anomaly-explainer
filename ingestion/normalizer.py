from __future__ import annotations 

import json

from pathlib import Path
from typing import Any
from uuid import UUID

from rapidfuzz import fuzz

from ingestion.models import RawInvoice
from schemas.columns_mapping import ColumnMappingResult
from schemas.invoice import Invoice, InvoiceLineItem



class Normalizer:
    """
    Normalizes raw csv into definied pydantic models.
    
    Args:
        data: list of RawInvoice object from CSVParser
        confidence_threshold: confidence threshold for mapping from config/settings.py
    """
    
    def __init__(self, data: list[RawInvoice], confidence_threshold: float = 0.0) -> None:
        if len(data) == 0:
            raise ValueError("Provided data must contain at least 1 row!")
        self._data = data
        self._confidence_threshold = confidence_threshold
        

    def normalize(self, path: str|Path) -> tuple[Invoice, list[InvoiceLineItem]]:
        """
        Runs full normalization pipeline.
        Loads columns_mapping, maps raw columns to it using exact -> fuzzy -> llm cascade. 
        Applies it to all raw data rows.
        Then, creates Pydantic (SQLModel) objects - Invoice and list with InvoiceLineItem.
        
        Args:
            path: path to columns_mapping
        
        Returns:
            Tuple of pydantic objects - Invoice and list of InvoiceLineItem
        """
        
        columns_mapping = self._read_columns_mapping_json(path)
        
        single_row = self._data[0]
        raw_columns = list(single_row.model_dump().keys())
        
        mapping_exact = self._map_columns(raw_columns, columns_mapping)
        
        resolved_exact = {r.raw_column: r.schema_field for r in mapping_exact if r.resolved}
        unresolved_schema_fields = list(set(columns_mapping.keys()) - set(resolved_exact.values()))
        unresolved_raw_fields = list(set(raw_columns) - set(resolved_exact.keys()))
        
        mapping_fuzzy = []
        if unresolved_schema_fields or unresolved_raw_fields:
            mapping_fuzzy = self._fuzzy_match_columns(
                self._confidence_threshold,
                columns_mapping,
                unresolved_schema_fields,
                unresolved_raw_fields 
            )       
        
        mapping_combined = [r for r in mapping_exact if r.resolved] + mapping_fuzzy
        mapped_data = self._apply_mapping(mapping_combined)
        
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


    def _apply_mapping(self, mapped_cols: list[ColumnMappingResult]) -> list[dict[str, Any]]:
        """
        Applies mapping from _map_columns to all data.
        
        Args:
            mapped_cols: result from _map_columns
        
        Returns:
            List of dicts with row data mapped to desired column names
        """
        results = []
        mapping = {result.raw_column: result.schema_field for result in mapped_cols if result.resolved}
        
        for row in self._data:
            row_dict = {}
            
            for col, value in row.model_dump().items():
                mapped_col = mapping.get(col.lower())
                
                if mapped_col is None:
                    if "invoice_metadata" not in row_dict:
                        row_dict["invoice_metadata"] = {}
                        
                    row_dict["invoice_metadata"][col] = value
                else:
                    row_dict[mapped_col] = value
                    
            results.append(row_dict)

        return results          


    @staticmethod
    def _map_columns(raw_columns: list, mapping: dict) -> list[ColumnMappingResult]:
        """
        Maps columns from RawInvoice to SQL models using columns_mapping.
        
        Args:
            raw_columns: columns from RawInvoice
            mapping: nested dict, columns_mapping.json
        
        Returns:
            List with ColumnMappingResult containing mapping and its metadata
        """
        results = []
        raw_columns_lower = [col.lower() for col in raw_columns]
        seen = {}
            
        for raw_col in raw_columns_lower:
            
            result = ColumnMappingResult(
                raw_column=raw_col, 
                schema_field=None, 
                method="exact", 
                resolved=False, 
                confidence=None
            )
            
            for key, values in mapping.items():
                possible_names = values.get("possible_names", [])
                
                if raw_col in possible_names:
                    
                    if raw_col in seen:
                        raise ValueError(f"Schema field '{key}' already mapped to'{seen[key]}', cannot also match '{raw_col}'")
                    seen[key] = raw_col
                    result = ColumnMappingResult(
                        raw_column=raw_col, 
                        schema_field=key, 
                        method="exact", 
                        resolved=True, 
                        confidence=None
                    )
                    break
            results.append(result)
                    
        return results
    

    @staticmethod
    def _fuzzy_match_columns(
        threshold: float,
        mapping: dict,
        unresolved_schema_fields: list,
        unresolved_raw_fields: list,
    ) -> list[ColumnMappingResult]:
        """
        Performs fuzzy match between raw_col and possible_names of schema_field using WRatio.
        Compares only unresolved cases of raw_fields and unresolved schema_fields from previous step (exact search using _map_search())
        
        Args:
            threshold: threshold to accept fuzzy match, set in settings.py
            mapping: nested dict, columns_mapping.json
            unresolved_schema_fields: unresolved schema_fields (desired cols) from exact search
            unresolved_raw_fields: unresolved raw_fields (inserted cols) from exact search
        
        Returns:
            List with ColumnMappingResult containing mapping and its metadata
        """
        
        results = []
        seen = set()
        
        for raw_col in unresolved_raw_fields:
            best_score = 0
            best_match = None
            
            for schema_col in unresolved_schema_fields:
                possible_names = mapping.get(schema_col, {}).get("possible_names", [])
    
                for name in possible_names:
                    score = fuzz.WRatio(raw_col, name) / 100 # original returns 0-100

                    if score > best_score and schema_col not in seen:
                        best_score = score
                        best_match = schema_col
                        
            if best_score >= threshold:
                seen.add(best_match)
                results.append(
                    ColumnMappingResult(
                        raw_column=raw_col, 
                        schema_field=best_match, 
                        method="fuzzy", 
                        resolved=True, 
                        confidence=best_score
                    )
                )

            else:
                results.append(
                    ColumnMappingResult(
                        raw_column=raw_col, 
                        schema_field=None, 
                        method="fuzzy", 
                        resolved=False, 
                        confidence=None
                    )
                )
        
        return results
      

    # @staticmethod
    # async def _llm_match_columns(
    #     ollama_url: str,
    #     model_name: str,
    #     raw_columns: list, 
    #     mapping: dict,
    # ) -> dict[str, str]:
    
    @staticmethod
    def _read_columns_mapping_json(path: str|Path) -> dict[str, dict]:
        """
        Loads columns_mapping for exact string comparison and mapping. 
        From raw provided cols to defined in SQL model.
        
        Args:
            path: path to columns_mapping
            
        Returns:
            Nested dict with desired cols (keys), info as dict about col (values)
        """
        path_converted = Path(path)
        
        if not path_converted.is_file():
            raise FileNotFoundError(f"There is no file in provided path: {path_converted}") 
        
        with open(path_converted) as f:
            return json.load(f)

        
                    
            

