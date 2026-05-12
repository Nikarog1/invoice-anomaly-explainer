import json
from uuid import uuid4

from pipeline.nodes.completeness_check_ingestion import completeness_check_ingestion
from pipeline.state import PipelineState
from schemas.anomaly import Severity, Source
from schemas.columns_mapping import ColumnMappingResult, MappingMethod



def test_completeness_check_ingestion_returns_expected_output(fake_session) -> None:
    invoice_id = uuid4()
    column_mapping_results = [
        ColumnMappingResult(
            invoice_id=invoice_id,
            raw_column="invoice number",
            schema_field="invoice_number",
            method=MappingMethod.exact,
            resolved=True,
            confidence=None,    
        ),
        ColumnMappingResult(
            invoice_id=invoice_id,
            raw_column="suppl.",
            schema_field="supplier_name",
            method=MappingMethod.fuzzy,
            resolved=True,
            confidence=0.9,    
        ),
        ColumnMappingResult(
            invoice_id=invoice_id,
            raw_column="tot sum",
            schema_field="total_amount",
            method=MappingMethod.llm,
            resolved=True,
            confidence=0.6,    
        ),
        ColumnMappingResult(
            invoice_id=invoice_id,
            raw_column="VAT identifier",
            schema_field=None,
            method=MappingMethod.llm,
            resolved=False,
            confidence=None,    
        ),
    ]
    fake_session.add_all(column_mapping_results)
    fake_session.commit()
    
    state: PipelineState = {"invoice_id": invoice_id} # type: ignore[typeddict-item]
    
    result = completeness_check_ingestion(state)
    flag = result["anomaly_flags"][0]
    
    assert flag.anomaly_report_id is None
    assert flag.invoice_id == invoice_id
    assert flag.anomaly_name == Source.completeness_check_ingestion
    assert flag.anomaly_severity == Severity.yellow
    assert flag.anomaly_source == Source.completeness_check_ingestion
    assert flag.anomaly_deviation is None
    
    assert flag.anomaly_notes is not None
    notes = json.loads(flag.anomaly_notes)
    
    assert len(notes["unresolved"]) == 1
    assert len(notes["resolved_by_llm"]) == 1
    assert len(notes["resolved_by_fuzzy"]) == 1
    
    notes_unresolved = next(n for n in notes["unresolved"] if n["raw_column"] == "VAT identifier")
    assert notes_unresolved["raw_column"] == "VAT identifier"
    assert notes_unresolved["schema_field"] is None
    assert notes_unresolved["method"] == "llm"
    assert not notes_unresolved["resolved"]
    assert notes_unresolved["confidence"] is None
    
    notes_llm = next(n for n in notes["resolved_by_llm"] if n["raw_column"] == "tot sum")
    assert notes_llm["raw_column"] == "tot sum"
    assert notes_llm["schema_field"] == "total_amount"
    assert notes_llm["method"] == "llm"
    assert notes_llm["resolved"]
    assert notes_llm["confidence"] == 0.6
    
    notes_fuzzy = next(n for n in notes["resolved_by_fuzzy"] if n["raw_column"] == "suppl.")
    assert notes_fuzzy["raw_column"] == "suppl."
    assert notes_fuzzy["schema_field"] == "supplier_name"
    assert notes_fuzzy["method"] == "fuzzy"
    assert notes_fuzzy["resolved"]
    assert notes_fuzzy["confidence"] == 0.9
    

def test_completeness_check_ingestion_mapping_not_found_exception_returns_empty_list(fake_session) -> None:
    invoice_id = uuid4()
    invoice_id_mapping = uuid4()
    column_mapping_results = [
        ColumnMappingResult(
            invoice_id=invoice_id_mapping,
            raw_column="invoice number",
            schema_field="invoice_number",
            method=MappingMethod.exact,
            resolved=True,
            confidence=None,    
        ),
        ColumnMappingResult(
            invoice_id=invoice_id_mapping,
            raw_column="supplier_name",
            schema_field="supplier_name",
            method=MappingMethod.exact,
            resolved=True,
            confidence=None,    
        ),
    ]
    fake_session.add_all(column_mapping_results)
    fake_session.commit()
    state: PipelineState = {"invoice_id": invoice_id} # type: ignore[typeddict-item]
    
    result = completeness_check_ingestion(state)
    
    assert result["anomaly_flags"] == []
    

def test_completeness_check_ingestion_all_resolved_exact_returns_empty_list(fake_session) -> None:
    invoice_id = uuid4()
    column_mapping_results = [
        ColumnMappingResult(
            invoice_id=invoice_id,
            raw_column="invoice number",
            schema_field="invoice_number",
            method=MappingMethod.exact,
            resolved=True,
            confidence=None,    
        ),
        ColumnMappingResult(
            invoice_id=invoice_id,
            raw_column="supplier_name",
            schema_field="supplier_name",
            method=MappingMethod.exact,
            resolved=True,
            confidence=None,    
        ),
    ]
    fake_session.add_all(column_mapping_results)
    fake_session.commit()
    
    state: PipelineState = {"invoice_id": invoice_id} # type: ignore[typeddict-item]
    
    result = completeness_check_ingestion(state)
    
    assert result["anomaly_flags"] == []