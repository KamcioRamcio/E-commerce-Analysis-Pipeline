{{ config(
    materialized = 'table',
    tags = ['core']
) }}

SELECT 
    c.CustomerID,
    COUNT(DISTINCT c.InvoiceNo) AS total_orders,
    SUM(c.Quantity) AS total_items_purchased,
    SUM(c.SalesAmount) AS total_spent,
    MIN(c.InvoiceDate) AS first_purchase_date,
    MAX(c.InvoiceDate) AS last_purchase_date,
    r.RFM_Segment,
    r.recency,
    r.frequency,
    r.monetary
FROM {{ source('dbt_project', 'online_retail_27_cleaned') }} c
LEFT JOIN {{ source('dbt_project', 'rfm_online_retail_27') }} r
    ON c.CustomerID = r.CustomerID
WHERE c.CustomerID IS NOT NULL
GROUP BY c.CustomerID, r.RFM_Segment, r.recency, r.frequency, r.monetary