from pipeline.state import PipelineState

from core.exceptions import InvoiceMappingNotFoundError
from core.logging import get_logger
from ingestion.repository import IngestionRepository

from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.columns_mapping import ColumnMapping, IngestionMappingNotes, MappingMethod

logger = get_logger(__name__)



def completeness_check_ingestion(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    """
    Load column mapping records for invoice_id from ingestion phase.
    Group uncertain mappings (unresolved, resolved by LLM, resolved by fuzzy) into IngestionMappingNotes.
    Produce single yellow AnomalyFlag with notes serialized to JSON when any uncertain mapping exists.
    Return empty flag list when all mappings are exact or when mapping records are missing.

    Side effects:
        Logs error and returns empty flag list if InvoiceMappingNotFoundError is caught.
    """
    logger.info("Running completeness_check_ingestion")
    invoice_id = state["invoice_id"]
    
    try:
        column_mapping_results = IngestionRepository().load_mappings(invoice_id=invoice_id)
    except InvoiceMappingNotFoundError:
        logger.error(f"No ingestion mappings found for invoice {invoice_id} — skipping completeness check")
        return {"anomaly_flags": []}
    
    column_mapping_results_py = [ColumnMapping(**m.model_dump()) for m in column_mapping_results] 
    
    notes = IngestionMappingNotes(
        unresolved=[m for m in column_mapping_results_py if not m.resolved],
        resolved_by_llm=[m for m in column_mapping_results_py if m.method == MappingMethod.llm and m.resolved],
        resolved_by_fuzzy=[m for m in column_mapping_results_py if m.method == MappingMethod.fuzzy],
    )
    
    if not (notes.unresolved or notes.resolved_by_llm or notes.resolved_by_fuzzy):
        return {"anomaly_flags": []}
    
    flag = AnomalyFlag(
        anomaly_report_id=None,
        invoice_id=invoice_id,
        anomaly_name=Source.completeness_check_ingestion,
        anomaly_severity=Severity.yellow,
        anomaly_source=Source.completeness_check_ingestion,
        anomaly_deviation=None,
        anomaly_notes=notes.model_dump_json(),
    )
    logger.info(f"Anomaly flag raised! unresolved={len(notes.unresolved)}, llm={len(notes.resolved_by_llm)}, fuzzy={len(notes.resolved_by_fuzzy)}")
    
    return {
        "anomaly_flags": [flag]
    }