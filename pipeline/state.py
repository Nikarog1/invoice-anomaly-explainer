import operator
from typing import Annotated, TypedDict
from uuid import UUID

from schemas.anomaly import AnomalyFlag, AnomalyReport
from schemas.contract import ContractSummary
from schemas.history import HistoricalSummary
from schemas.junction import LineItemMatch
from schemas.invoice import Invoice, InvoiceLineItem



class PipelineState(TypedDict):
    invoice_id: UUID # input
    invoice: Invoice | None
    invoice_line_items: list[InvoiceLineItem] | None
    historical_summary: HistoricalSummary | None
    contract_summary: ContractSummary | None 
    line_item_matches: list[LineItemMatch]
    anomaly_flags: Annotated[list[AnomalyFlag], operator.add]
    agent_report: AnomalyReport | None