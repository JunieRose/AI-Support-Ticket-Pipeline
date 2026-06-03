/* 
    AI Categorization vs. Customer Sentiment
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
    Volume by Severity & Operational Efficiency
    Goal: Monitor ticket distribution and ensure urgent issues are prioritized.
*/

SELECT 
    severity, 
    COUNT(*) as ticket_count,
    ROUND(AVG(response_latency_hrs), 2) as avg_latency_hrs
FROM support_tickets
GROUP BY severity
ORDER BY CASE severity 
    WHEN 'P1-URGENT' THEN 1 
    WHEN 'P2-HIGH' THEN 2 
    WHEN 'P3-NORMAL' THEN 3 
    ELSE 4 END;

/* 
    Escalation Risk by Region
    Goal: Geographical breakdown of high-risk tickets to identify regional service gaps.
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
    SLA First Response Performance
    Goal: Track adherence to response time targets.
*/

SELECT 
    CASE 
        WHEN response_latency_hrs <= 1 THEN 'Under 1hr'
        WHEN response_latency_hrs <= 4 THEN '1-4hrs (Target)'
        WHEN response_latency_hrs <= 8 THEN '4-8hrs (Delayed)'
        ELSE 'Over 8hrs (SLA Breach)'
    END as response_window,
    COUNT(*) as ticket_count
FROM support_tickets
GROUP BY 
    CASE 
        WHEN response_latency_hrs <= 1 THEN 'Under 1hr'
        WHEN response_latency_hrs <= 4 THEN '1-4hrs (Target)'
        WHEN response_latency_hrs <= 8 THEN '4-8hrs (Delayed)'
        ELSE 'Over 8hrs (SLA Breach)'
    END
ORDER BY MIN(response_latency_hrs);

/* 
    Daily Ticket Trend
    Goal: Visualize daily volume spikes to assist in workforce planning.
*/

SELECT 
    TO_CHAR(created_at, 'YYYY-MM-DD') as created_date, 
    COUNT(*) as daily_volume
FROM SUPPORT_TICKETS 
GROUP BY created_date 
ORDER BY created_date;
