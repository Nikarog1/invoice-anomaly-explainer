from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict



class InvoiceParseAndNormalizeSettings(BaseModel):
    langs_string_search: list = ["EN", "CZ", "GR", "SP", "FR"]
    invoice_fuzzy_match_min: float = 0.85
    
class ThresholdSettings(BaseModel):
    stats_deviation: float = 2.0
    fuzzy_match_min: float = 0.85
    vector_sim_min: float = 0.80

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )
    ollama_base_url: str = "http://localhost:11434"
    model_name: str = "mistral"
    sqlite_path: Path = Path("./data/invoices.db")
    chromadb_path: Path = Path("./data/chroma")
    fastapi_host: str = "127.0.0.1"
    fastapi_port: int = 8000
    thresholds: ThresholdSettings = ThresholdSettings()
    invoice_ingestion: InvoiceParseAndNormalizeSettings = InvoiceParseAndNormalizeSettings()
    
settings = Settings() # fail fast approach

