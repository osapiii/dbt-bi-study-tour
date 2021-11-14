{{ config(materialized='view') }}

SELECT
    id,
    status,
    payment_method,
    -- 【典型前処理① コード変換】ステータスの日本語変換ver 
    CASE payment_method
    WHEN 'coupon' || 'debit_card' THEN 'Not Subsription' ELSE 'Subscription' END AS payment_method_category,
    amount
FROM
    {{ ref('raw_charges') }}