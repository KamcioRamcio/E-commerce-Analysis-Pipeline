{{ config(
    materialized = 'table',
    tags = ['core']
) }}

SELECT
    s.InvoiceNo,
    s.InvoiceDate,
    s.CustomerID,
    s.Country,
    s.Quantity,
    s.UnitPrice,
    s.StockCode,
    s.Description,
    s.SalesAmount,
    s.SalesDate,
    s.SalesTime,
    s.SalesMonth,
    s.SalesDayOfWeek,
    CASE
        WHEN r.transaction_status IN ('matched_return', 'orphan_return')
        THEN TRUE
        ELSE FALSE
    END AS is_return_processed,
    r.transaction_status
FROM {{ ref('stg_invoices') }} s
LEFT JOIN {{ ref('fct_returns_analysis') }} r
    ON s.InvoiceNo = r.transaction_id
WHERE
    (r.transaction_status IS NULL OR r.transaction_status != 'unmatched_return')
    AND s.isCorrection = FALSE
