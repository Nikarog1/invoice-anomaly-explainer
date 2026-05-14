from pydantic import BaseModel, Field
from enum import Enum



class DegradationReason(str, Enum):
    window_miss = "window_miss"
    thin_count = "thin_count"
    no_history = "no_history"

class LineItemStatsAmount(BaseModel):
    description: str # same as description in InvoiceLineItem
    mean_amount: float
    stddev_amount: float | None # None if num of cases < 2
    n_samples: int
    
class LineItemStatsUnitPrice(BaseModel):
    description: str # same as description in InvoiceLineItem
    mean_price: float
    stddev_price: float | None # None if num of cases < 2
    n_samples: int

class HistoricalSummary(BaseModel):
    supplier_name: str
    invoice_count: int
    fields_seen: set[str]  # which fields have historically appeared; potentially will be redeveloped to allow thresholds of appearance
    metadata_keys_seen: set[str] # which fields have historically appeared in invoice_metadata
    line_item_stats_amount: list[LineItemStatsAmount]
    line_item_stats_unit_price: list[LineItemStatsUnitPrice]
    is_degraded: bool
    degradation_reason: DegradationReason | None = None
    
class HistoricalCompletenessNotes(BaseModel):
    missing_universal_fields: set[str]
    new_universal_fields: set[str]
    missing_metadata_keys: set[str]
    new_metadata_keys: set[str]

class PriceField(str, Enum):
    unit_price = "unit_price"
    amount_gross = "amount_gross"
    
class HistoricalStatsLine(BaseModel):
    description: str
    price_field: PriceField
    amount: float
    historical_mean: float
    historical_stddev: float | None
    z_score: float | None # (amount - mean) / stddev
    deviation: float | None
    
class HistoricalStatsNotes(BaseModel):
    anomalous_lines: list[HistoricalStatsLine]
    
class UnmatchedLineNotes(BaseModel):
    unmatched_lines: set[str]