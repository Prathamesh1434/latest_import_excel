-- ════════════════════════════════════════════════════════════
-- sql/init_context_store.sql
-- Run ONCE to add the context cache table to your Oracle DB
-- Additive — does not touch any existing tables
-- ════════════════════════════════════════════════════════════

CREATE TABLE BI_DATA_CONTEXT (
    ID              NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_ID       VARCHAR2(200)   NOT NULL,
    SOURCE_NAME     VARCHAR2(500),
    CHUNK_ID        VARCHAR2(50)    NOT NULL,
    CHUNK_TYPE      VARCHAR2(50)    NOT NULL,   -- summary | schema | stats | rows
    CONTENT         CLOB            NOT NULL,
    TOKEN_COUNT     NUMBER          DEFAULT 0,
    METADATA        CLOB,                       -- JSON: filters, view_id, extracted_at
    TOTAL_ROWS      NUMBER          DEFAULT 0,
    TOTAL_COLS      NUMBER          DEFAULT 0,
    EXTRACTED_DT    TIMESTAMP       DEFAULT SYSTIMESTAMP,
    EXPIRES_DT      TIMESTAMP,
    CONSTRAINT UQ_CONTEXT_CHUNK UNIQUE (SOURCE_ID, CHUNK_ID)
);

COMMENT ON TABLE  BI_DATA_CONTEXT          IS 'Cached Tableau data chunks for LLM context injection';
COMMENT ON COLUMN BI_DATA_CONTEXT.SOURCE_ID  IS 'Internal scorecard ID (e.g. uk-kri, crmr-cde)';
COMMENT ON COLUMN BI_DATA_CONTEXT.CHUNK_TYPE IS 'summary | schema | stats | rows';
COMMENT ON COLUMN BI_DATA_CONTEXT.CONTENT    IS 'LLM-ready text chunk';
COMMENT ON COLUMN BI_DATA_CONTEXT.EXPIRES_DT IS 'Cache expiry — NULL means never expires';

CREATE INDEX IDX_DATA_CONTEXT_SOURCE  ON BI_DATA_CONTEXT (SOURCE_ID);
CREATE INDEX IDX_DATA_CONTEXT_EXPIRES ON BI_DATA_CONTEXT (EXPIRES_DT);
CREATE INDEX IDX_DATA_CONTEXT_TYPE    ON BI_DATA_CONTEXT (CHUNK_TYPE);

-- ════════════════════════════════════════════════════════════
-- Cleanup job (run manually or schedule via DBMS_SCHEDULER)
-- ════════════════════════════════════════════════════════════

-- DELETE FROM BI_DATA_CONTEXT WHERE EXPIRES_DT < SYSTIMESTAMP;
-- COMMIT;
