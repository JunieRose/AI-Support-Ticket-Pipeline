-- Project: AI-Support-Ticket-Pipeline
-- Author: Junie Rose Bodoso
-- Description: DDL to create the support_tickets table with Virtual Columns for SLA tracking.
-- Requirements: Oracle Autonomous Database

CREATE TABLE support_tickets (
    -------------------------------------------------------------------------
    -- Core Ticket Data
    -------------------------------------------------------------------------

    ticket_id            VARCHAR2(100) PRIMARY KEY,
    created_at           TIMESTAMP,
    customer_text        VARCHAR2(1000),
    region               VARCHAR2(50),
    first_response_at    TIMESTAMP,

    -------------------------------------------------------------------------
    -- AI Enrichment Columns
    -------------------------------------------------------------------------

    sentiment            NUMBER, 
    category             VARCHAR2(50),
    analysis_source      VARCHAR2(50),
    
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

    -- B. Severity (Virtual - based on raw sentiment)
    severity VARCHAR2(20) GENERATED ALWAYS AS (
        CASE 
            WHEN sentiment <= -0.6 THEN 'P1-URGENT'
            WHEN sentiment < 0    THEN 'P2-HIGH'
            WHEN sentiment >= 0.5 THEN 'P4-LOW'
            ELSE 'P3-NORMAL'
        END
    ) VIRTUAL,

    -- C. Escalation Risk (Repeating the math to avoid ORA-54012)
    escalation_risk VARCHAR2(20) GENERATED ALWAYS AS (
        CASE
            -- Ticket still awaiting first response
            WHEN first_response_at IS NULL
                THEN 'PENDING RESPONSE'
            
            -- Negative sentiment + slow response
            WHEN sentiment < 0
                AND (
                    EXTRACT(DAY FROM (first_response_at - created_at)) * 24 +
                    EXTRACT(HOUR FROM (first_response_at - created_at)) +
                    EXTRACT(MINUTE FROM (first_response_at - created_at)) / 60
                ) > 4 
                THEN 'CRITICAL'
                 
            -- High response latency
            WHEN (EXTRACT(DAY FROM (first_response_at - created_at)) * 24 +
                  EXTRACT(HOUR FROM (first_response_at - created_at)) +
                  EXTRACT(MINUTE FROM (first_response_at - created_at)) / 60
                ) > 8 
                THEN 'HIGH'

            ELSE 'ON TRACK'
        END
    ) VIRTUAL,

    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
