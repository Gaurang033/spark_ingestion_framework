-- ============================================================================
-- Ingestion Framework - Audit Table Setup
-- ============================================================================
-- 
-- Purpose: Creates the ingestion_audit_info table for tracking pipeline runs
-- 
-- This table is shared across ALL tables in the schema and tracks:
-- - Sequential ingestion_id per table
-- - Records inserted/rejected per run
-- - Timestamps and status
-- 
-- IMPORTANT: Run this BEFORE your first pipeline execution
-- 
-- Usage:
--   1. Update the catalog and schema to match your pipeline settings
--   2. Run this SQL in a SQL editor or notebook
--   3. Verify table creation: DESCRIBE demo.bronze.ingestion_audit_info;
-- 
-- ============================================================================

-- Update these values to match your pipeline target catalog/schema
USE CATALOG demo;
USE SCHEMA bronze;

-- Create the audit metadata table
CREATE TABLE IF NOT EXISTS ingestion_audit_info (
    table_name STRING 
        COMMENT 'Name of the target table being ingested',
    
    status STRING 
        COMMENT 'Ingestion status: SUCCESS or FAILED',
    
    ingestion_id BIGINT 
        COMMENT 'Sequential ingestion ID per table (starts at 0)',
    
    records_inserted BIGINT 
        COMMENT 'Number of valid records successfully inserted',
    
    records_rejected BIGINT 
        COMMENT 'Number of corrupt/invalid records rejected',
    
    pipeline_update_id STRING 
        COMMENT 'Databricks pipeline update ID for traceability',
    
    start_timestamp TIMESTAMP 
        COMMENT 'Timestamp when ingestion started',
    
    end_timestamp TIMESTAMP 
        COMMENT 'Timestamp when ingestion completed',
    
    error_message STRING 
        COMMENT 'Error message if ingestion failed, NULL if successful'
)
COMMENT 'Audit metadata tracking all pipeline ingestions in this schema';

-- Verify table creation
DESCRIBE ingestion_audit_info;

-- Show current audit history (empty on first run)
SELECT * FROM ingestion_audit_info ORDER BY table_name, ingestion_id DESC;
