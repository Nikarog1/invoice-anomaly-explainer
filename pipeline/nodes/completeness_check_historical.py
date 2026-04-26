from pipeline.state import PipelineState

from core.exceptions import PipelineStateError
from core.logging import get_logger

from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.history import HistoricalCompletenessNotes

logger = get_logger(__name__)



def completeness_check_historical(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    """
    Compare current invoice's universal fields and metadata keys against historical_summary.
    Identify missing fields (in history, not in current invoice) and new fields (in current, not in history).
    Group differences into HistoricalCompletenessNotes split by universal vs metadata.
    Produce single yellow AnomalyFlag with notes serialized to JSON when any difference exists.
    Return empty flag list when current invoice's fields match historical baseline exactly.

    Raises:
        PipelineStateError: if invoice, invoice_line_items, or historical_summary missing from state.
    """
    
    logger.info("Running completeness_check_historical")
    invoice_id = state["invoice_id"]
    invoice = state["invoice"]
    invoice_line_items = state["invoice_line_items"]
    historical_summary = state["historical_summary"]
    
    if (
        invoice is None
        or invoice_line_items is None
        or historical_summary is None
    ):
        raise PipelineStateError("invoice, or/and invoice_line_items, or/and historical_summary")
    
    fields_seen = (
        set(k for k, v in invoice.model_dump().items() if v is not None) 
        | set(k for line_item in invoice_line_items for k, v in line_item.model_dump().items() if v is not None)
    )
    metadata_keys_seen = set(k for k, v in invoice.invoice_metadata.items() if v is not None)
    
    missing_universal_fields = historical_summary.fields_seen - fields_seen
    new_universal_fields = fields_seen - historical_summary.fields_seen
    missing_metadata_keys = historical_summary.metadata_keys_seen - metadata_keys_seen
    new_metadata_keys = metadata_keys_seen - historical_summary.metadata_keys_seen
    
    if not(missing_universal_fields or new_universal_fields or missing_metadata_keys or new_metadata_keys):
        return {"anomaly_flags": []}
    
    notes = HistoricalCompletenessNotes(
        missing_universal_fields=missing_universal_fields,
        new_universal_fields=new_universal_fields,
        missing_metadata_keys=missing_metadata_keys,
        new_metadata_keys=new_metadata_keys,
    )
    flag = AnomalyFlag(
        anomaly_report_id=None,
        invoice_id=invoice_id,
        anomaly_name=Source.completeness_check_historical,
        anomaly_severity=Severity.yellow,
        anomaly_source=Source.completeness_check_historical,
        anomaly_deviation=None,
        anomaly_notes=notes.model_dump_json(),
    )
    logger.info(
        f"Anomaly flag raised! "
        f"missing_universal={len(notes.missing_universal_fields)}, "
        f"new_universal={len(notes.new_universal_fields)}, "
        f"missing_metadata={len(notes.missing_metadata_keys)}, "
        f"new_metadata={len(notes.new_metadata_keys)}"
    )
    
    return {
        "anomaly_flags": [flag]
    }
    

        