{{ config(
    materialized = 'table',
    tags = ['core']
) }}

SELECT
    StockCode,
    Description,
    COUNT(DISTINCT InvoiceNo) AS total_orders,
    SUM(Quantity) AS total_quantity_sold,
    SUM(SalesAmount) AS total_revenue
FROM {{ source('dbt_project', 'online_retail_27_cleaned') }}
WHERE StockCode IS NOT NULL AND Description IS NOT NULL
GROUP BY StockCode, Description