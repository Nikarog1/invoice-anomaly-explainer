from typing import Literal

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from pipeline.state import PipelineState
from pipeline.nodes.load_invoice import load_invoice
from pipeline.nodes.completeness_check_ingestion import completeness_check_ingestion
from pipeline.nodes.load_past_invoices import load_past_invoices
from pipeline.nodes.completeness_check_historical import completeness_check_historical
from pipeline.nodes.statistical_vs_history import statistical_vs_history
from pipeline.nodes.load_contract import load_contract
from pipeline.nodes.contract_matching import contract_matching
from pipeline.nodes.statistical_vs_contract import statistical_vs_contract
from pipeline.agents.agent_explanation import explanation
from pipeline.nodes.delivery import delivery

from schemas.history import DegradationReason as DR_History



def check_historical_available(state: PipelineState) -> Literal["has_history", "no_history"]:
    return (
        "has_history" 
        if state["historical_summary"]
        and state["historical_summary"].degradation_reason != DR_History.no_history
        else "no_history")
    
def check_contract_available(state: PipelineState) -> Literal["has_contract", "no_contract"]:
    return (
        "has_contract" 
        if state["contract_summary"]
        and not state["contract_summary"].is_degraded
        else "no_contract")


def build_graph(checkpointer=None) -> CompiledStateGraph:
    builder = StateGraph(PipelineState)
    builder.add_node("load_invoice", load_invoice)
    builder.add_node("completeness_check_ingestion", completeness_check_ingestion)
    builder.add_node("load_past_invoices", load_past_invoices)
    builder.add_node("completeness_check_historical", completeness_check_historical)
    builder.add_node("statistical_vs_history", statistical_vs_history)
    builder.add_node("load_contract", load_contract)
    builder.add_node("contract_matching", contract_matching)
    builder.add_node("statistical_vs_contract", statistical_vs_contract)
    builder.add_node("explanation", explanation)
    builder.add_node("delivery", delivery)

    builder.add_edge(START, "load_invoice")
    builder.add_edge("load_invoice", "completeness_check_ingestion")
    builder.add_edge("completeness_check_ingestion", "load_past_invoices")
    builder.add_conditional_edges(
        "load_past_invoices", 
        check_historical_available, 
        {"has_history": "completeness_check_historical", "no_history": "load_contract"}
    )
    builder.add_edge("completeness_check_historical", "statistical_vs_history")
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

    return builder.compile(checkpointer=checkpointer)