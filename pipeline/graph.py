from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from core.logging import get_logger

from pipeline.state import PipelineState

from schemas.anomaly import AnomalyFlag, Severity, Source
from schemas.contract import Contract, ContractLineItem, ContractWithLineItems
from schemas.history import HistoricalSummary, LineItemStats
from schemas.junction import LineItemMatch
from schemas.junction import Method
from schemas.invoice import Invoice, InvoiceLineItem

logger = get_logger(__name__)



def load_invoice(state: PipelineState) -> dict:
    logger.info("Running load_invoice")
    invoice_id = state["invoice_id"]
    return {
        "invoice": Invoice(invoice_id=invoice_id, invoice_number="123", supplier_name="suppl1", total_amount=990.0),
        "invoice_line_items": [
            InvoiceLineItem(invoice_id=invoice_id, description="smth1", amount_gross=500.0),
            InvoiceLineItem(invoice_id=invoice_id, description="smth2", amount_gross=490.0),
        ]
    }

def load_past_invoices(state: PipelineState) -> dict:
    logger.info("Running load_past_invoices")
    return {
        "historical_summary": HistoricalSummary(
            supplier_name = "suppl1",
            invoice_count = 10,
            fields_seen = {"invoice_number", "supplier_name", "total_amount", "description", "amount_gross", "issue_date"},
            line_item_stats = [
                LineItemStats(description="smth1", mean_amount=300, stddev_amount=50, n_samples=7),
                LineItemStats(description="smth2", mean_amount=1500, stddev_amount=200, n_samples=5),
            ]
        )
    }

def completeness_check(state: PipelineState) -> dict:
    logger.info("Running completeness_check")
    invoice_id = state["invoice_id"]
    return {
        "anomaly_flags": [AnomalyFlag(
            anomaly_report_id=None, 
            invoice_id=invoice_id, 
            anomaly_name="Missing field",
            anomaly_severity=Severity.red,
            anomaly_source=Source.completeness_check,
            )
        ]
    }

def statistical_vs_history(state: PipelineState) -> dict:
    logger.info("Running statistical_vs_history")
    invoice_id = state["invoice_id"]
    return {
        "anomaly_flags": [AnomalyFlag(
            anomaly_report_id=None, 
            invoice_id=invoice_id, 
            anomaly_name="Historical deviation found",
            anomaly_severity=Severity.red,
            anomaly_source=Source.statistical_vs_history,
            )
        ]
    }

def load_contract(state: PipelineState) -> dict:
    logger.info("Running load_contract")
    contract_id = uuid4()
    return {
        "contracts": [
                ContractWithLineItems(
                contract = Contract(contract_id=contract_id, supplier_name="suppl1", buyer_name="our_company"),
                line_items = [
                    ContractLineItem(contract_id=contract_id, product_service_name="smth1", unit_price=300),
                    ContractLineItem(contract_id=contract_id, product_service_name="smth2", unit_price=1400),
                ]
            )
        ]
    }

def contract_matching(state: PipelineState) -> dict:
    logger.info("Running contract_matching")
    return {
        "line_item_matches": [
            LineItemMatch(contract_line_item_id=uuid4(), invoice_line_item_id=uuid4(), match_method=Method.exact, match_score=1.0),
            LineItemMatch(contract_line_item_id=uuid4(), invoice_line_item_id=uuid4(), match_method=Method.vector, match_score=0.8),
        ]
    }

def statistical_vs_contract(state: PipelineState) -> dict:
    logger.info("Running statistical_vs_contract")
    invoice_id = state["invoice_id"]
    return {
        "anomaly_flags": [AnomalyFlag(
            anomaly_report_id=None, 
            invoice_id=invoice_id, 
            anomaly_name="Contract deviation found",
            anomaly_severity=Severity.red,
            anomaly_source=Source.statistical_vs_contract,
            )
        ]
    }

def explanation(state: PipelineState) -> dict:
    logger.info("Running explanation")
    invoice_id = state["invoice_id"]
    return {
        "explanation": f"some explanation on invoice ID: {invoice_id}",
        "explanation_datetime": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    }

def delivery(state: PipelineState) -> dict:
    logger.info("Running delivery")
    return {}

def check_historical_available(state: PipelineState) -> Literal["has_history", "no_history"]:
    return "has_history" if state["historical_summary"] is not None else "no_history"
    
def check_contract_available(state: PipelineState) -> Literal["has_contract", "no_contract"]:
    return "has_contract" if state["contracts"] is not None else "no_contract"



builder = StateGraph(PipelineState)
builder.add_node("load_invoice", load_invoice)
builder.add_node("load_past_invoices", load_past_invoices)
builder.add_node("completeness_check", completeness_check)
builder.add_node("statistical_vs_history", statistical_vs_history)
builder.add_node("load_contract", load_contract)
builder.add_node("contract_matching", contract_matching)
builder.add_node("statistical_vs_contract", statistical_vs_contract)
builder.add_node("explanation", explanation)
builder.add_node("delivery", delivery)

builder.add_edge(START, "load_invoice")
builder.add_edge("load_invoice", "load_past_invoices")
builder.add_conditional_edges(
    "load_past_invoices", 
    check_historical_available, 
    {"has_history": "completeness_check", "no_history": "load_contract"}
)
builder.add_edge("completeness_check", "statistical_vs_history")
builder.add_edge("statistical_vs_history", "load_contract")
builder.add_conditional_edges(
    "load_contract", 
    check_contract_available,
    {"has_contract": "contract_matching", "no_contract": "explanation"}
)
builder.add_edge("contract_matching", "statistical_vs_contract")
builder.add_edge("statistical_vs_contract", "explanation")
builder.add_edge("explanation", "delivery")
builder.add_edge("delivery", END)