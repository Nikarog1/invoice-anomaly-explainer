from sqlmodel import select

from config.settings import settings

from core.exceptions import PipelineStateError
from core.logging import get_logger

from data.sqlite import get_session

from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, AnomalousStatisticalLine, AnomalousStatisticalNotes, Metric, Severity, Source
from schemas.contract import ContractLineItem
from schemas.junction import LineItemMatch

logger = get_logger(__name__)



def statistical_vs_contract(state: PipelineState) -> dict[str, list[AnomalyFlag]]:
    
    logger.info("Running statistical_vs_contract")
    invoice_id = state["invoice_id"]
    invoice_line_items = state["invoice_line_items"]
    contract_summary = state["contract_summary"]
    
    if (
        invoice_line_items is None
        or contract_summary is None
    ):
        raise PipelineStateError("invoice_line_items or/and contract_summary")
    
    contract_candidates: list[ContractLineItem] = [
        item
        for contract in contract_summary.contracts
        for item in contract.line_items
    ]
    
    with get_session() as session:
        line_item_match = session.exec(
            select(LineItemMatch)
            .where(LineItemMatch.contract_line_item_id.in_([con.contract_line_item_id for con in contract_candidates])) # type: ignore
            .where(LineItemMatch.invoice_line_item_id.in_([inv.invoice_line_item_id for inv in invoice_line_items])) # type: ignore
        ).all()
        
    if not line_item_match:
        logger.warning("No contract matches found. Skipping statistical_vs_contract.")
        return {"anomaly_flags": []}
    
    anomalous_price = []
    anomalous_quantity = []
    
    for row in line_item_match:
        inv_line = next((inv for inv in invoice_line_items if inv.invoice_line_item_id == row.invoice_line_item_id), None)
        con_line = next((con for con in contract_candidates if con.contract_line_item_id == row.contract_line_item_id), None)
        
        if (
            inv_line 
            and con_line
            and inv_line.unit_price
        ):
            deviation = (inv_line.unit_price - con_line.unit_price) / con_line.unit_price
            
            if abs(deviation) >= settings.thresholds.default_contract_dev_threshold:
                anomalous_price.append(
                    AnomalousStatisticalLine(
                        description=inv_line.description,
                        invoice=inv_line.unit_price,
                        contract=con_line.unit_price,
                        deviation=deviation,
                        metric=Metric.unit_price
                    )
                )
        
        if (
            inv_line 
            and con_line
            and inv_line.quantity
            and con_line.max_units
        ):
            if inv_line.quantity > con_line.max_units:
                deviation = (inv_line.quantity - con_line.max_units) / con_line.max_units
                anomalous_quantity.append(
                    AnomalousStatisticalLine(
                        description=inv_line.description,
                        invoice=inv_line.quantity,
                        contract=con_line.max_units,
                        deviation=deviation,
                        metric=Metric.quantity
                    )
                )

    flags = []
            
    if anomalous_price:
        notes_price = AnomalousStatisticalNotes(anomalous_lines=anomalous_price)
        flag_price = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="unit_price_deviation",
            anomaly_severity=Severity.red,
            anomaly_source=Source.statistical_vs_contract,
            anomaly_deviation=None,
            anomaly_notes=notes_price.model_dump_json(),   
        )
        flags.append(flag_price)
        
    if anomalous_quantity:
        notes_quantity = AnomalousStatisticalNotes(anomalous_lines=anomalous_quantity)
        flag_quantity = AnomalyFlag(
            anomaly_report_id=None,
            invoice_id=invoice_id,
            anomaly_name="quantity_deviation",
            anomaly_severity=Severity.red,
            anomaly_source=Source.statistical_vs_contract,
            anomaly_deviation=None,
            anomaly_notes=notes_quantity.model_dump_json(),   
        )
        flags.append(flag_quantity)
        
    return {
        "anomaly_flags": flags
    }
        
        
                
    
