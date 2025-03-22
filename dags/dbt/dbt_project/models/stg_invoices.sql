{{ config(
    materialized = 'view',
    tags = ['clean']
) }}

WITH base_data AS (
    SELECT
        InvoiceNo,
        InvoiceDate,
        CustomerID,
        Country,
        Quantity,
        UnitPrice,
        StockCode,
        Description,
        isCorrection,
        SalesDate,
        SalesTime,
        SalesAmount,
        SalesMonth,
        SalesDayOfWeek
    FROM {{ source('dbt_project', 'online_retail_27_cleaned') }}
    WHERE
        CustomerID != -1
        AND isSpecialCode = FALSE
)

SELECT * FROM base_data