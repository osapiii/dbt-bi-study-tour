
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

SELECT
    -- from fixed_shopify_orders
    ---- 発注ID
    fixed_shopify_orders.id AS order_id, 
    ---- 発注日 
    fixed_shopify_orders.date,
    ---- 発注日(日時)
    fixed_shopify_orders.date_cast_to_datetime,
    ---- 発注日(日時 JST)
    fixed_shopify_orders.date_in_jst,
    ---- 顧客ID
    fixed_shopify_orders.customer_id,
    ---- 発注ステータス
    fixed_shopify_orders.status,
    ---- 発注ステータス(JP)
    fixed_shopify_orders.status_jp,
    ---- 決済ID
    fixed_shopify_orders.payment_id,
    ---- 購入アイテム数
    fixed_shopify_orders.item_count,
    ---- 決済内での同時購入アイテム数
    fixed_shopify_orders.total_item_count_in_payment,
    ---- 顧客の合計購入アイテム数
    fixed_shopify_orders.total_item_count_in_customer,
    ---- 購入時点での顧客の累計購入アイテム数
    fixed_shopify_orders.cumsum_item_count_in_customer,

    -- from fixed_charges
    ---- 決済方法
    fixed_charges.payment_method,
    ---- 決済方法カテゴリ
    fixed_charges.payment_method_category,
    ---- 決済金額
    fixed_charges.amount,

    -- from fixed_crm_customers
    ---- 姓
    fixed_crm_customers.first_name,
    ---- 名
    fixed_crm_customers.last_name,
    ---- フルネーム
    fixed_crm_customers.full_name,
    ---- 暗号化済みEメール
    fixed_crm_customers.hashed_email
FROM
    {{ ref('fixed_shopify_orders') }} AS fixed_shopify_orders
LEFT OUTER JOIN
    {{ ref('fixed_charges') }} AS fixed_charges
ON
    fixed_shopify_orders.payment_id = fixed_charges.id
LEFT OUTER JOIN
    {{ ref('fixed_crm_customers') }} AS fixed_crm_customers
ON
    fixed_shopify_orders.customer_id = fixed_crm_customers.id