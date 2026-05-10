from sqlalchemy.exc import SQLAlchemyError

from core.exceptions import PipelineStateError, PipelineRepositoryError
from core.logging import get_logger
from data.sqlite import get_session
from pipeline.state import PipelineState

logger = get_logger(__name__)



def delivery(state: PipelineState) -> dict:
    logger.info("Running delivery")
    
    anomaly_count = len(state["anomaly_flags"])
    line_item_count = len(state["line_item_matches"])
    agent_report = state["agent_report"]
    
    if agent_report is None:
        raise PipelineStateError("agent_report")
    
    agent_explanation = agent_report.agent_explanation
    
    logger.info("Writing results to db")
    with get_session() as session:
        
        session.add(agent_report)
        
        anomaly_report_id = agent_report.anomaly_report_id
        for anomaly in state["anomaly_flags"]:
            anomaly.anomaly_report_id = anomaly_report_id
            session.add(anomaly)
            
        try:
            session.commit()
        except SQLAlchemyError as e:
            logger.exception("Delivery write failed")
            raise PipelineRepositoryError(agent_report.invoice_id) from e
        
    logger.info(f"Successfully wrote 1 anomaly report and {anomaly_count} anomaly flag{"s" if anomaly_count != 1 else ""} to db")
    logger.info(f"Successfully wrote {line_item_count} line item match{"es" if line_item_count != 1 else ""} to db")
    logger.info(f"Agent explanation: {agent_explanation}")
    
    return {
        "anomaly_report": agent_report
    }