from pydantic import BaseModel



class LineItemStats(BaseModel):
    description: str # same as description in InvoiceLineItem
    mean_amount: float
    stddev_amount: float
    n_samples: int

class HistoricalSummary(BaseModel):
    supplier_name: str
    invoice_count: int
    fields_seen: set[str]  # which fields have historically appeared
    line_item_stats: list[LineItemStats]