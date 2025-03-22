{{ config(
    materialized = 'table',
    tags = ['core']
) }}

SELECT
    FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_TRUNC(TIMESTAMP(InvoiceDate), MONTH)) AS sale_month,
    COUNT(DISTINCT InvoiceNo) AS orders_count,
    COUNT(DISTINCT CustomerID) AS unique_customers,
    SUM(Quantity) AS items_sold,
    SUM(Quantity * UnitPrice) AS monthly_revenue
FROM {{ source('dbt_project', 'online_retail_27_cleaned') }}
WHERE InvoiceDate IS NOT NULL
GROUP BY FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_TRUNC(TIMESTAMP(InvoiceDate), MONTH))
ORDER BY sale_month