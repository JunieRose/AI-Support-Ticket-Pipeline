-- Project: AI-Support-Ticket-Pipeline
-- Author: Junie Rose Bodoso
-- Description: DDL to create the support_tickets table with Virtual Columns for SLA tracking.
-- Requirements: Oracle Autonomous Database

CREATE TABLE support_tickets (
    -------------------------------------------------------------------------
    -- Core Ticket Data
    -------------------------------------------------------------------------

    ticket_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email_address        VARCHAR2(100) NOT NULL,
    created_at           TIMESTAMP NOT NULL,
    customer_text        VARCHAR2(1000) NOT NULL,
    region_id            NUMBER CONSTRAINT fk_region_id REFERENCES DIM_REGIONS(region_id), 
    first_response_at    TIMESTAMP,

    -------------------------------------------------------------------------
    -- AI Enrichment Columns
    -------------------------------------------------------------------------

    sentiment            NUMBER NOT NULL, 
    category_id          NUMBER CONSTRAINT fk_category_id REFERENCES DIM_CATEGORIES(category_id),
    analysis_source      VARCHAR2(50) NOT NULL,
    
    -------------------------------------------------------------------------
    -- VIRTUAL COLUMNS: The "Business Logic" Layer
    -------------------------------------------------------------------------
    
    -- A. Latency Calculation (Virtual)
    response_latency_hrs NUMBER GENERATED ALWAYS AS (
        CASE 
            WHEN first_response_at IS NULL THEN NULL
            ELSE ROUND(
                EXTRACT(DAY FROM (first_response_at - created_at)) * 24 +
                EXTRACT(HOUR FROM (first_response_at - created_at)) +
                EXTRACT(MINUTE FROM (first_response_at - created_at)) / 60, 
            2)
        END
    ) VIRTUAL,

    -- B. Severity (Virtual - based on raw sentiment_score)
    severity VARCHAR2(20) GENERATED ALWAYS AS (
        CASE 
            WHEN sentiment_score <= -0.6 THEN 'P1-URGENT'
            WHEN sentiment_score < 0    THEN 'P2-HIGH'
            WHEN sentiment_score >= 0.5 THEN 'P4-LOW'
            ELSE 'P3-NORMAL'
        END
    ) VIRTUAL,

    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);