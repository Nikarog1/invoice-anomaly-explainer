COLUMN_MAPPING_PROMPT  = """
You are a column name mapper in an invoice normalization pipeline.

TASK:
Map each raw column name to the correct schema field. Raw column names may be in ANY language or use abbreviations.

RULES:
- Only map to schema fields provided below. Never invent new field names.
- If a raw column clearly represents a schema field (even in another language), map it.
- If the raw column does not clearly correspond to any schema field, return null.
- Each schema field can only be assigned to one raw column. If multiple raw columns seem to match the same field, pick the most certain one and return null / other suitable schema fields for the others.
- Return ONLY valid JSON. No explanation, no markdown, no extra text.

RAW COLUMN NAMES:
{raw_column_names}

SCHEMA FIELDS (name → description + known synonyms):
{mapping}

OUTPUT FORMAT:
Return a single JSON object where each key is a raw column name and each value is either a schema field name or null.

EXAMPLE:
{{"factura_numero": "invoice_number", "sales_tax_rate": "vat_rate", "internal_cost_center": null}}
"""