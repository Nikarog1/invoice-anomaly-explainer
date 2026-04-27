from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict



class InvoiceParseAndNormalizeSettings(BaseModel):
    langs_string_search: list[str] = ["EN", "CZ", "GR", "SP", "FR"]
    invoice_fuzzy_match_min: float = 0.85

class SuppliersHistoricalInvoices(BaseModel):
    default_history_window_months: int = 12
    default_min_samples: int = 3     # 3 to 6 is the best interval

class ThresholdSettings(BaseModel):
    stats_deviation: float = 2.0
    fuzzy_match_min: float = 0.85
    vector_sim_min: float = 0.80
    default_z_score_threshold: float = 3

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )
    columns_mapping_path: Path = Path("./config/columns_mapping.json")
    ollama_base_url: str = "http://localhost:11434"
    model_name: str = "mistral"
    sqlite_url: str = "sqlite:///./data/data.db"
    chromadb_path: Path = Path("./data/chroma")
    fastapi_host: str = "127.0.0.1"
    fastapi_port: int = 8000
    thresholds: ThresholdSettings = ThresholdSettings()
    invoice_ingestion: InvoiceParseAndNormalizeSettings = InvoiceParseAndNormalizeSettings()
    suppliers_config: SuppliersHistoricalInvoices = SuppliersHistoricalInvoices()
    
settings = Settings() # fail fast approach

