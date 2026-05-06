CREATE TABLE silver_tickets (
    ticket_id VARCHAR(100) PRIMARY KEY,
    created_date TIMESTAMP,
    customer_text CLOB,
    region VARCHAR2(50),
    first_response TIMESTAMP,
    resolved_at TIMESTAMP,
    sentiment NUMBER,
    category VARCHAR2(50),
    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolution_hours NUMBER GENERATED ALWAYS AS (
        ROUND(
            EXTRACT(DAY FROM (resolved_at - created_date)) * 24 +
            EXTRACT(HOUR FROM (resolved_at - created_date)) +
            EXTRACT(MINUTE FROM (resolved_at - created_date)) / 60
        )
    ) VIRTUAL
);
