{{ config(
    materialized = 'table',
    tags = ['core']
) }}

WITH
original_invoices AS (
    SELECT *
    FROM {{ ref('stg_invoices') }}
    WHERE isCorrection = FALSE
),

returned_invoices AS (
    SELECT *
    FROM {{ ref('stg_invoices') }}
    WHERE isCorrection = TRUE
),

matched_returns AS (
    SELECT
        o.InvoiceNo AS original_invoice,
        r.InvoiceNo AS return_invoice,
        COALESCE(o.CustomerID, r.CustomerID) AS CustomerID,
        COALESCE(o.Country, r.Country) AS Country,
        COALESCE(o.StockCode, r.StockCode) AS StockCode,
        COALESCE(o.Description, r.Description) AS Description,
        o.Quantity AS original_quantity,
        r.Quantity AS returned_quantity,
        o.SalesAmount AS original_amount,
        r.SalesAmount AS return_amount,
        CASE
            WHEN o.CustomerID = r.CustomerID
                 AND o.StockCode = r.StockCode
                 AND o.SalesAmount = ABS(r.SalesAmount) THEN 'exact_match'
            WHEN o.StockCode = r.StockCode
                 AND o.SalesAmount = ABS(r.SalesAmount) THEN 'item_match'
            ELSE 'no_match'
        END AS match_type
    FROM original_invoices o
    FULL OUTER JOIN returned_invoices r
        ON o.CustomerID = r.CustomerID
        AND o.StockCode = r.StockCode
        AND o.SalesAmount = ABS(r.SalesAmount)
        AND o.Country = r.Country
),

final_matching AS (
    SELECT
        COALESCE(original_invoice, return_invoice) AS transaction_id,
        CustomerID,
        Country,
        StockCode,
        Description,
        original_quantity,
        returned_quantity,
        original_amount,
        return_amount,
        match_type,
        CASE
            WHEN match_type = 'no_match' THEN 'unmatched_return'
            WHEN original_invoice IS NULL THEN 'orphan_return'
            WHEN return_invoice IS NULL THEN 'valid_sale'
            ELSE 'matched_return'
        END AS transaction_status
    FROM matched_returns
)

SELECT * FROM final_matching
