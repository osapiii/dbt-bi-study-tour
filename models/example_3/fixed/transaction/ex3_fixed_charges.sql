
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

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