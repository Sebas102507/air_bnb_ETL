-- Bronze Schema: For raw, unprocessed source data from various systems.
CREATE SCHEMA IF NOT EXISTS bronze;

-- Silver Schema: For cleaned, validated, and enriched data.
CREATE SCHEMA IF NOT EXISTS silver;

-- Gold Schema: For aggregated, business-ready data ready for analytics.
CREATE SCHEMA IF NOT EXISTS gold;