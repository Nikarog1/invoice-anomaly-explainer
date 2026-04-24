from pathlib import Path
from uuid import UUID

from config.settings import settings
from core.logging import get_logger

from ingestion.csv_parser import CSVParser
from ingestion.normalizer import Normalizer
from ingestion.repository import IngestionRepository

logger = get_logger(__name__)



class IngestionService:
    async def run(
            self,
            path_to_csv: Path | str,
            path_to_mapping: Path | str = settings.columns_mapping_path,
            confidence_threshold: float = settings.invoice_ingestion.invoice_fuzzy_match_min,
            ollama_url: str = settings.ollama_base_url,
            model_name: str = settings.model_name,
    ) -> UUID:
        """
        Run whole ingestion pipeline to extract, normalize, and save for further processing.
        
        Args:
            path_to_csv: path to invoice in csv format
            path_to_mapping: path to columns_mapping
            confidence_threshold: confidence threshold for fuzzy match mapping from config/settings.py
            ollama_url: ollama url of local model
            model_name: model name performing validation
        """
        logger.info(f"Ingestion started for {path_to_csv}")
        raw_fields = CSVParser(path=path_to_csv).parse()
        
        normalizer = Normalizer(
            raw_fields,
            path=path_to_mapping,
            confidence_threshold=confidence_threshold,
            ollama_url=ollama_url,
            model_name=model_name,
        )
        ingestion_result = await normalizer.normalize()
        invoice_id = ingestion_result.invoice.invoice_id
        
        IngestionRepository().save(ingestion_result=ingestion_result)
        logger.info(f"Ingestion complete for {invoice_id}")
        
        return invoice_id