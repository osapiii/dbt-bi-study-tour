
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

-- 【典型前処理① ルールベースの除外 - 定義】特定の顧客IDを除外する
WITH
    invalid_costomer_ids AS (
        --　9と18がテスト用顧客という設定
        SELECT 9
        UNION ALL
        SELECT 18
)

SELECT
    id,
    date,
    -- 【典型前処理① 日付変換】日付のフォーマット変換
    CAST(date AS DATETIME) AS date_cast_to_datetime,
    DATE_ADD(CAST(date AS DATETIME), INTERVAL 9 HOUR) AS date_in_jst,
    customer_id,
    status,
    -- 【典型前処理① コード変換】ステータスの日本語変換ver
    CASE status
    WHEN 'success' THEN '購買完了'
    WHEN 'pending' THEN '購入処理中'
    WHEN 'cancelled' THEN 'キャンセル' ELSE 'その他' END AS status_jp,
    payment_id,
    item_count,
    -- 【典型前処理① 集計用のフラグ追加】単一決済内での購買アイテム数
    SUM(item_count) OVER (PARTITION BY payment_id) AS total_item_count_in_payment,
    -- 顧客の合計購買回数
    COUNT(*) OVER (PARTITION BY customer_id) AS total_item_count_in_customer,
    -- 顧客の累計購買回数
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS cumsum_item_count_in_customer
FROM
    {{ ref('raw_shopify_orders') }}
WHERE
    -- 【典型前処理① ルールベースの除外 - 除外】特定の顧客IDを除外する
    CAST(customer_id AS INT64) NOT IN (SELECT * FROM invalid_costomer_ids)
ORDER BY
    id