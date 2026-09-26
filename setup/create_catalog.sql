-- Unity Catalog objects for the GCN Fermi lakehouse.
-- Run once per workspace, before the first Bronze ingest.

CREATE CATALOG IF NOT EXISTS kafka_nasa_fermi
  COMMENT 'NASA GCN Fermi GBM gamma-ray burst notices';

CREATE SCHEMA IF NOT EXISTS kafka_nasa_fermi.bronze
  COMMENT 'Raw classic-text notices as delivered by the GCN Kafka broker';

CREATE SCHEMA IF NOT EXISTS kafka_nasa_fermi.silver
  COMMENT 'Parsed and typed notice fields, plus quarantined malformed records';

CREATE SCHEMA IF NOT EXISTS kafka_nasa_fermi.gold
  COMMENT 'Snowflake dimensional model for analysis';

-- Streaming checkpoints. A managed volume keeps them under Unity Catalog
-- governance alongside the tables they belong to.
CREATE VOLUME IF NOT EXISTS kafka_nasa_fermi.bronze.checkpoints
  COMMENT 'Structured Streaming checkpoint locations';
