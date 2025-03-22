{{ config(
    materialized = 'table',
    tags = ['analytics']
) }}

SELECT
    InvoiceNo,
    InvoiceDate,
    CustomerID,
    Country,
    COUNT(StockCode) AS unique_items,
    SUM(Quantity) AS total_items,
    SUM(SalesAmount) AS invoice_total
FROM {{ source('dbt_project', 'online_retail_27_cleaned') }}
GROUP BY InvoiceNo, InvoiceDate, CustomerID, Country