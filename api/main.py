from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn

from api.exception_handlers import register_exception_handlers
from api.routes import invoices

from config.settings import settings
from core.logging import get_logger

from data.llm_client import verify_ollama_models
from data.sqlite import create_db_and_tables

logger = get_logger(__name__)



@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — creating tables")
    create_db_and_tables()
    
    logger.info("Verifying Ollama models")
    await verify_ollama_models()
    
    yield
    
    logger.info("Shutting down")
    
app = FastAPI(title="invoice_anomaly_explainer", lifespan=lifespan)
register_exception_handlers(app)

app.include_router(invoices.router)

if __name__ == "__main__":
    uvicorn.run("api.main:app", host=settings.fastapi_host, port=settings.fastapi_port, reload=True)