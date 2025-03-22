{{ config(
    materialized = 'table',
    tags = ['core']
) }}

SELECT
    Country,
    COUNT(InvoiceNo) as total_inovices,
FROM {{ ref('fct_clean_sales') }}
GROUP BY Country