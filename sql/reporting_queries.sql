/* 
    Volume by Severity & Operational Efficiency
    Goal: Monitor ticket distribution and ensure urgent issues are prioritized.
*/

SELECT 
    severity,
    COUNT(*) AS ticket_count,
    COUNT(first_response_at) AS responded_tickets,
    COUNT(*) - COUNT(first_response_at) AS unanswered_tickets,
    ROUND(AVG(response_latency_hrs), 2) AS avg_latency_hrs
FROM support_tickets
GROUP BY severity
ORDER BY CASE severity
    WHEN 'P1-URGENT' THEN 1
    WHEN 'P2-HIGH' THEN 2
    WHEN 'P3-NORMAL' THEN 3
    ELSE 4
END;

/* 
    Category vs. Sentiment Analysis
    Goal: Identify which ticket categories are driving negative customer experiences.
*/

SELECT 
    category,
    ROUND(AVG(sentiment), 2) as avg_sentiment_score,
    COUNT(*) as volume
FROM support_tickets
GROUP BY category
ORDER BY avg_sentiment_score ASC;

/* 
    Escalation Risk by Region
    Goal: Breakdown of high-risk tickets to identify regional service gaps.
*/

SELECT 
    region,
    COUNT(CASE WHEN escalation_risk = 'CRITICAL' THEN 1 END) as critical_count,
    COUNT(CASE WHEN escalation_risk = 'HIGH' THEN 1 END) as high_risk_count,
    COUNT(*) as total_tickets
FROM support_tickets
GROUP BY region
ORDER BY critical_count DESC

/* 
    Open vs. Responded Tickets
    Goal: Track the proportion of tickets that have received a response to identify potential backlog issues.
*/

SELECT
    CASE
        WHEN first_response_at IS NULL THEN 'Open'
        ELSE 'Responded'
    END AS ticket_status,
    COUNT(*) AS total_tickets
FROM support_tickets
GROUP BY
    CASE
        WHEN first_response_at IS NULL THEN 'Open'
        ELSE 'Responded'
    END

/* 
    AI Processing Source Monitoring
    Goal: Track adherence to response time targets.
*/

SELECT
    analysis_source,
    COUNT(*) AS total_records
FROM support_tickets
GROUP BY analysis_source

/* 
    Hourly Ticket Trend
    Goal: Visualize hourly volume spikes to assist in workforce planning.
*/

SELECT 
    TO_CHAR(TRUNC(created_at, 'HH'), 'YYYY-MM-DD HH24:MI') AS created_date, 
    count(*)
FROM SUPPORT_TICKETS 
GROUP BY created_date
ORDER BY created_date
