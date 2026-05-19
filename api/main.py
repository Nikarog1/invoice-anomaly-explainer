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