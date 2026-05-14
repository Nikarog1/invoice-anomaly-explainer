from config.settings import settings
from core.exceptions import PipelineStateError
from core.logging import get_logger
from pipeline.state import PipelineState
from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.history import HistoricalStatsLine, HistoricalStatsNotes, PriceField, UnmatchedLineNotes

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
    invoice_line_items = state["invoice_line_items"]
    historical_summary = state["historical_summary"]
    
    if (
        invoice_line_items is None
        or historical_summary is None
    ):
        raise PipelineStateError("invoice_line_items or/and historical_summary")
    
    line_item_stats_amount = historical_summary.line_item_stats_amount
    line_item_stats_unit = historical_summary.line_item_stats_unit_price
    
    anomalous_lines = []
    unmatched_lines = set()
    
    for item in invoice_line_items:
        history_amount = next((line for line in line_item_stats_amount if line.description == item.description), None)
        history_unit = next((line for line in line_item_stats_unit if line.description == item.description), None)
        
        if not history_amount:
            unmatched_lines.add(item.description)
            
        else:
            unit = False
            
            current_price = item.amount_gross
            history_price = history_amount.mean_amount
            history_stddev = history_amount.stddev_amount
            
            if item.unit_price is not None and history_unit is not None:
                unit = True
                current_price = item.unit_price
                history_price = history_unit.mean_price
                history_stddev = history_unit.stddev_price
                
            deviation = (current_price - history_price) / history_price if history_price != 0 else None

            if history_stddev is None:
                z_score = None
                
            elif history_stddev == 0 and history_price != 0:
                z_score = 0
                
            elif history_stddev == 0:
                z_score = None
                
            else:
                z_score = (current_price - history_price) / history_stddev

            flag_line = HistoricalStatsLine(
                description=item.description,
                price_field=PriceField.unit_price if unit else PriceField.amount_gross,
                amount=current_price,
                historical_mean=history_price,
                historical_stddev=history_stddev,
                z_score=z_score,
                deviation=deviation,
            )

            if z_score is not None and abs(z_score) >= settings.thresholds.default_z_score_threshold:
                anomalous_lines.append(flag_line)
            elif deviation is not None and abs(deviation) >= settings.thresholds.default_history_dev_threshold:
                anomalous_lines.append(flag_line)
                
    flags = []
           
    if anomalous_lines:
        notes_stats = HistoricalStatsNotes(anomalous_lines=anomalous_lines)
        flag_statistical = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="historical_deviation",
            anomaly_severity=Severity.yellow if historical_summary.is_degraded else Severity.red,
            anomaly_source=Source.statistical_vs_history,
            anomaly_deviation=None,
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
            anomaly_deviation=None,
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
        
