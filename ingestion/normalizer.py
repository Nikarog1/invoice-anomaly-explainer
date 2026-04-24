from __future__ import annotations 

import json

from pathlib import Path
from typing import Any
from uuid import UUID

from httpx import AsyncClient

from rapidfuzz import fuzz

from config.prompts import COLUMN_MAPPING_PROMPT
from core.logging import get_logger

from ingestion.models import IngestionResult, RawInvoice

from schemas.columns_mapping import ColumnMapping, MappingMethod
from schemas.invoice import Invoice, InvoiceLineItem

logger = get_logger(__name__)



class Normalizer:
    """
    Normalizes raw csv into definied pydantic models.
    
    Args:
        data: list of RawInvoice object from CSVParser
        path: path to columns_mapping
        confidence_threshold: confidence threshold for fuzzy match mapping from config/settings.py
        ollama_url: ollama url of local model
        model_name: model name performing validation
    """
    
    def __init__(
            self, 
            data: list[RawInvoice], 
            path: str | Path = "./config/columns_mapping.json",
            confidence_threshold: float = 0.8,
            ollama_url: str = "http://localhost:11434",
            model_name: str = "mistral",
        ) -> None:
        
        logger.debug(f"Normalizer initialized with threshold={confidence_threshold}, model={model_name}")
        
        if len(data) == 0:
            raise ValueError("Provided data must contain at least 1 row!")
        
        self._data = [
            RawInvoice(**{k.lower(): v for k, v in row.model_dump().items()})
            for row in data
        ]
        self._columns_mapping = self._read_columns_mapping_json(path)
        self._confidence_threshold = confidence_threshold
        self._ollama_url = ollama_url
        self._model_name = model_name
        

    async def normalize(self) -> IngestionResult:
        """
        Runs full normalization pipeline.
        Loads columns_mapping, maps raw columns to it using exact -> fuzzy -> llm cascade. 
        Applies it to all raw data rows.
        Then, creates Pydantic (SQLModel) objects - Invoice and list with InvoiceLineItem.
        
        Returns:
            IngestionResult PyDantic model holding invoice, invoice_line_items, and column_mapping_results
        """
        
        logger.info(f"Starting normalization, {len(self._data)} rows")
        single_row = self._data[0]
        raw_columns = list(single_row.model_dump().keys())
        
        mapping_exact = self._map_columns(raw_columns, self._columns_mapping)
        
        resolved_fields = {r.raw_column: r.schema_field for r in mapping_exact if r.resolved}
        unresolved_schema_fields = list(set(self._columns_mapping.keys()) - set(resolved_fields.values()))
        unresolved_raw_fields = list(set(raw_columns) - set(resolved_fields.keys()))
        logger.info(f"Exact match resolved {len(resolved_fields)}/{len(raw_columns)} columns")
        
        mapping_fuzzy = []
        mapping_llm = []
        
        if unresolved_schema_fields or unresolved_raw_fields:
            mapping_fuzzy = self._fuzzy_match_columns(
                self._confidence_threshold,
                self._columns_mapping,
                unresolved_schema_fields,
                unresolved_raw_fields, 
            )
            logger.info(f"Fuzzy match resolved {len([r for r in mapping_fuzzy if r.resolved])} additional columns")
            
            all_resolved = {r.raw_column: r.schema_field for r in mapping_exact + mapping_fuzzy if r.resolved}
            unresolved_schema_fields = list(set(self._columns_mapping.keys()) - set(all_resolved.values()))
            unresolved_raw_fields = list(set(raw_columns) - set(all_resolved.keys()))
            
            if unresolved_schema_fields or unresolved_raw_fields:
                try: 
                    mapping_llm = await self._llm_match_columns(
                        self._ollama_url,
                        self._model_name,
                        COLUMN_MAPPING_PROMPT,
                        self._columns_mapping,
                        unresolved_schema_fields,
                        unresolved_raw_fields,
                    )
                    logger.warning(f"LLM match resolved {len([r for r in mapping_llm if r.resolved])} columns — review recommended")
                except Exception as e:
                    logger.warning(f"Skipping LLM match, could not access Ollama: {e}")
        
        logger.warning(f"Unresolved columns going to invoice_metadata: {unresolved_raw_fields}")
        mapping_combined = (
            [r for r in mapping_exact if r.resolved]
            + [r for r in mapping_fuzzy if r.resolved]
            + [r for r in mapping_llm if r.resolved]
        )
        mapped_data = self._apply_mapping(mapping_combined)
        
        invoice = self._build_invoice(mapped_data[0])
        invoice_id = invoice.invoice_id

        invoice_line_items = self._build_line_items(mapped_data, invoice_id)
        
        return IngestionResult(
            invoice=invoice, 
            invoice_line_items=invoice_line_items, 
            column_mapping_results=mapping_combined
        )
    
    
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


    def _apply_mapping(self, mapped_cols: list[ColumnMapping]) -> list[dict[str, Any]]:
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
                mapped_col = mapping.get(col)
                
                if mapped_col is None:
                    if "invoice_metadata" not in row_dict:
                        row_dict["invoice_metadata"] = {}
                        
                    row_dict["invoice_metadata"][col] = value
                else:
                    row_dict[mapped_col] = value
                    
            results.append(row_dict)

        return results          


    @staticmethod
    def _map_columns(raw_columns: list, mapping: dict) -> list[ColumnMapping]:
        """
        Maps columns from RawInvoice to SQL models using columns_mapping.
        
        Args:
            raw_columns: columns from RawInvoice
            mapping: nested dict, columns_mapping.json
        
        Returns:
            List with ColumnMapping containing mapping and its metadata
        """
        results = []
        raw_columns_lower = [col.lower() for col in raw_columns]
        seen = {}
            
        for raw_col in raw_columns_lower:
            
            result = ColumnMapping(
                raw_column=raw_col, 
                schema_field=None, 
                method=MappingMethod.exact, 
                resolved=False, 
                confidence=None
            )
            
            for key, values in mapping.items():
                possible_names = values.get("possible_names", [])
                
                if raw_col in possible_names:
                    
                    if raw_col in seen:
                        raise ValueError(f"Schema field '{key}' already mapped to'{seen[key]}', cannot also match '{raw_col}'")
                    seen[key] = raw_col
                    result = ColumnMapping(
                        raw_column=raw_col, 
                        schema_field=key, 
                        method=MappingMethod.exact, 
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
    ) -> list[ColumnMapping]:
        """
        Performs fuzzy match between raw_col and possible_names of schema_field using WRatio.
        Compares only unresolved cases of raw_fields and unresolved schema_fields from previous step (exact search using _map_search()).
        
        Args:
            threshold: threshold to accept fuzzy match, set in settings.py
            mapping: nested dict, columns_mapping.json
            unresolved_schema_fields: unresolved schema_fields (desired cols) from exact search
            unresolved_raw_fields: unresolved raw_fields (inserted cols) from exact search
        
        Returns:
            List with ColumnMapping containing mapping and its metadata
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
                    ColumnMapping(
                        raw_column=raw_col, 
                        schema_field=best_match, 
                        method=MappingMethod.fuzzy, 
                        resolved=True, 
                        confidence=best_score
                    )
                )

            else:
                results.append(
                    ColumnMapping(
                        raw_column=raw_col, 
                        schema_field=None, 
                        method=MappingMethod.fuzzy, 
                        resolved=False, 
                        confidence=None
                    )
                )
        
        return results
      

    @staticmethod
    async def _llm_match_columns(
        ollama_url: str,
        model_name: str,
        prompt: str,
        mapping: dict,
        unresolved_schema_fields: list,
        unresolved_raw_fields: list,
    ) -> list[ColumnMapping]:
        """
        Last instance of cascade to map unresolved cases.
        Sends them with mapping and unassigned schema fields to LLM.
        LLM responds with json with unresolved raw_cols assigned to unassigned schema_fields.
        If it unsures where raw_col belongs to, it maps it to null.
        NOTE: confidence score for llm match is always 0.6.
        
        Args:
            ollama_url: ollama url of local model
            model_name: model name performing validation
            prompt: system prompt to map fields
            mapping: nested dict, columns_mapping.json
            unresolved_schema_fields: unresolved schema_fields (desired cols) from fuzzy match
            unresolved_raw_fields: unresolved raw_fields (inserted cols) from fuzzy match
        
        Returns:
            List with ColumnMapping containing mapping and its metadata
        """
        
        # TODO: extract LLMClient when explanation agent is implemented
        
        mapping_unresolved_fields = {
            key: value 
            for key, value in mapping.items() 
            if key in unresolved_schema_fields
        }
        prompt_formatted = prompt.format(
            raw_column_names=unresolved_raw_fields,
            mapping=mapping_unresolved_fields
        )
        
        async with AsyncClient() as client:
            request = client.build_request(
                "POST", 
                ollama_url, 
                json={ 
                    "model": model_name, 
                    "format": "json", 
                    "stream": False, 
                    "messages": [
                        {
                            "role": "user", 
                            "content": prompt_formatted
                        }
                    ] 
                }
            )
            response = await client.send(request)
            content = response.json()["message"]["content"]
            response_dict = json.loads(content)
        
        results = []
        for key, value in response_dict.items():
            
            if value and value not in unresolved_schema_fields: # hallucination check 
                value = None 
                
            results.append(
                ColumnMapping(
                    raw_column=key,
                    schema_field=value,
                    method=MappingMethod.llm,
                    resolved=True if value else False,
                    confidence=0.6 if value else None,
                )
            )
        
        return results        
    

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