{{ config(
    materialized = 'table',
    tags = ['core']
) }}

SELECT
    DATE(InvoiceDate) AS sale_date,
    COUNT(DISTINCT InvoiceNo) AS orders_count,
    COUNT(DISTINCT CustomerID) AS unique_customers,
    SUM(Quantity) AS items_sold,
    SUM(SalesAmount) AS daily_revenue
FROM {{ source('dbt_project', 'online_retail_27_cleaned') }}
GROUP BY DATE(InvoiceDate)
ORDER BY sale_date