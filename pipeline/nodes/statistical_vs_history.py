from config.settings import settings
from core.exceptions import PipelineStateError
from core.logging import get_logger
from pipeline.state import PipelineState
from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.history import HistoricalStatsLine, HistoricalStatsNotes, UnmatchedLineNotes

logger = get_logger(__name__)



def statistical_vs_history(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    """
    Compare current invoice line items against historical_summary line stats.
    For each line item, compute z-score vs historical mean/stddev when both exist.
    Produce two flags when applicable:
        - line_amount_deviation (red, downgraded to yellow when history degraded):
        lines exceeding configured z-score threshold, grouped in HistoricalStatsNotes.
        - unmatched_line_item (yellow): lines with no historical match by description,
        listed in UnmatchedLineNotes.
    Return empty flag list when invoice matches historical baseline.

    Raises:
        PipelineStateError: if invoice, invoice_line_items, or historical_summary missing from state.
    """
    
    logger.info("Running statistical_vs_history")
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
    
    line_item_stats = historical_summary.line_item_stats
    
    anomalous_lines = []
    unmatched_lines = set()
    
    for item in invoice_line_items:
        history = next((line for line in line_item_stats if line.description == item.description), None)
        
        if not history:
            unmatched_lines.add(item.description)
            
        else:
            
            if history.stddev_amount is None or history.stddev_amount == 0:
                z_score = None
            else:
                z_score = (item.amount_gross - history.mean_amount) / history.stddev_amount
                
            if z_score is not None and abs(z_score) >= settings.thresholds.default_z_score_threshold:
                anomalous_lines.append(
                    HistoricalStatsLine(
                        description=item.description,
                        amount_gross=item.amount_gross,
                        historical_mean=history.mean_amount,
                        historical_stddev=history.stddev_amount,
                        z_score=z_score,
                    )
                )
                
    flags = []
           
    if anomalous_lines:
        notes_stats = HistoricalStatsNotes(anomalous_lines=anomalous_lines)
        flag_statistical = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="line_amount_deviation",
            anomaly_severity=Severity.yellow if historical_summary.is_degraded else Severity.red,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None, # at the end, probably this argument is not useful if I aggregate everything in notes
            anomaly_notes=notes_stats.model_dump_json(),
        )
        flags.append(flag_statistical)
        
    if unmatched_lines:
        notes_unmatched = UnmatchedLineNotes(unmatched_lines=unmatched_lines)
        flag_unmatched = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="unmatched_line_item",
            anomaly_severity=Severity.yellow,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None, # at the end, probably this argument is not useful if I aggregate everything in notes
            anomaly_notes=notes_unmatched.model_dump_json(),
        )
        flags.append(flag_unmatched)
        
    if not flags:
        logger.info("No anomaly flag raised")
    else:
        logger.info(
            f"Anomaly flag raised! "
            f"statistical_anomaly={len(anomalous_lines)}, "
            f"unmatched={len(unmatched_lines)}"
        )
        
    return {
        "anomaly_flags": flags
    }
        
