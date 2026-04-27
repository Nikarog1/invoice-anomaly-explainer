from pydantic import BaseModel
from enum import Enum



class DegradationReason(str, Enum):
    window_miss = "window_miss"
    thin_count = "thin_count"
    no_history = "no_history"

class LineItemStats(BaseModel):
    description: str # same as description in InvoiceLineItem
    mean_amount: float
    stddev_amount: float | None # None if num of cases < 2
    n_samples: int

class HistoricalSummary(BaseModel):
    supplier_name: str
    invoice_count: int
    fields_seen: set[str]  # which fields have historically appeared; potentially will be redeveloped to allow thresholds of appearance
    metadata_keys_seen: set[str] # which fields have historically appeared in invoice_metadata
    line_item_stats: list[LineItemStats]
    is_degraded: bool
    degradation_reason: DegradationReason | None = None
    
class HistoricalCompletenessNotes(BaseModel):
    missing_universal_fields: set[str]
    new_universal_fields: set[str]
    missing_metadata_keys: set[str]
    new_metadata_keys: set[str]
    
class HistoricalStatsLine(BaseModel):
    description: str
    amount_gross: float
    historical_mean: float
    historical_stddev: float | None
    z_score: float | None # (amount - mean) / stddev
    
class HistoricalStatsNotes(BaseModel):
    anomalous_lines: list[HistoricalStatsLine]
    
class UnmatchedLineNotes(BaseModel):
    unmatched_lines: set[str]