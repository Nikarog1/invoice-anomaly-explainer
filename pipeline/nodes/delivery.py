from core.logging import get_logger
from data.sqlite import get_session
from pipeline.state import PipelineState
from schemas.anomaly import AnomalyReport

logger = get_logger(__name__)



def delivery(state: PipelineState) -> dict:
    logger.info("Running delivery")
    
    anomaly_count = len(state["anomaly_flags"])
    line_item_count = len(state["line_item_matches"])
    agent_explanation = state["agent_explanation"]
    

    anomaly_report = AnomalyReport(
        invoice_id=state["invoice_id"],
        anomalies_count=anomaly_count,
        agent_explanation=agent_explanation,
        explanation_date=state["explanation_datetime"]
    )
    
    logger.info("Writing results to db")
    with get_session() as session:
        
        session.add(anomaly_report)
        
        anomaly_report_id = anomaly_report.anomaly_report_id
        for anomaly in state["anomaly_flags"]:
            anomaly.anomaly_report_id = anomaly_report_id
            session.add(anomaly)
            
        for match in state["line_item_matches"]:
            session.add(match)
            
        try:
            session.commit()
        except Exception:
            logger.exception("Failed to write pipeline results to db")
            raise
        
    logger.info(f"Successfully wrote 1 anomaly report and {anomaly_count} anomaly flag{"s" if anomaly_count != 1 else ""} to db")
    logger.info(f"Successfully wrote {line_item_count} line item match{"es" if line_item_count != 1 else ""} to db")
    logger.info(f"Agent explanation: {agent_explanation}")
    
    return {
        "anomaly_report_id": anomaly_report_id
    }