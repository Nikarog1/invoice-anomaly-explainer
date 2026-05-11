from uuid import UUID

from langchain_core.runnables.config import RunnableConfig
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from config.settings import settings
from pipeline.graph import build_graph
from schemas.anomaly import AnomalyReport



async def run_pipeline(invoice_id: UUID) -> AnomalyReport:
    """Run the full pipeline for one invoice, return the final report."""
    async with AsyncSqliteSaver.from_conn_string(settings.checkpoint_path) as checkpointer:
        graph = build_graph(checkpointer=checkpointer)
        config: RunnableConfig = {"configurable": {"thread_id": str(invoice_id)}}
        final_state = await graph.ainvoke({"invoice_id": invoice_id}, config=config)
        return final_state["agent_report"]