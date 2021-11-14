WITH
-- crm_customersの前処理
crm_customers AS (
        SELECT
            id,
            first_name,
            last_name,
            -- 【典型前処理① 文字列整形】フルネームの取得
            CONCAT(first_name,' ',last_name) AS full_name,
            -- email,
            -- 【典型前処理① 難読化】メールアドレスのハッシュ化
            MD5(email) AS hashed_email, 
            country
        FROM
            {{ ref('ex1_raw_crm_customers') }}
),

-- 【典型前処理① ルールベースの除外 - 定義】特定の顧客IDを除外する
invalid_costomer_ids AS (
        --　9と18がテスト用顧客という設定
        SELECT 9
        UNION ALL
        SELECT 18
),

-- shopify_ordersの前処理
shopify_orders AS (
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
        {{ ref('ex1_raw_shopify_orders') }}
    WHERE
        -- 【典型前処理① ルールベースの除外 - 除外】特定の顧客IDを除外する
        CAST(customer_id AS INT64) NOT IN (SELECT * FROM invalid_costomer_ids)
    ORDER BY
        id
),

charges AS (
    SELECT
        id,
        status,
        payment_method,
        -- 【典型前処理① コード変換】ステータスの日本語変換ver 
        CASE payment_method
        WHEN 'coupon' || 'debit_card' THEN 'Not Subsription' ELSE 'Subscription' END AS payment_method_category,
        amount
    FROM
        {{ ref('ex1_raw_charges') }}
)

SELECT
    -- from fixed_shopify_orders
    ---- 発注ID
    shopify_orders.id AS order_id, 
    ---- 発注日 
    shopify_orders.date,
    ---- 発注日(日時)
    shopify_orders.date_cast_to_datetime,
    ---- 発注日(日時 JST)
    shopify_orders.date_in_jst,
    ---- 顧客ID
    shopify_orders.customer_id,
    ---- 発注ステータス
    shopify_orders.status,
    ---- 発注ステータス(JP)
    shopify_orders.status_jp,
    ---- 決済ID
    shopify_orders.payment_id,
    ---- 購入アイテム数
    shopify_orders.item_count,
    ---- 決済内での同時購入アイテム数
    shopify_orders.total_item_count_in_payment,
    ---- 顧客の合計購入アイテム数
    shopify_orders.total_item_count_in_customer,
    ---- 購入時点での顧客の累計購入アイテム数
    shopify_orders.cumsum_item_count_in_customer,

    -- from fixed_charges
    ---- 決済方法
    charges.payment_method,
    ---- 決済方法カテゴリ
    charges.payment_method_category,
    ---- 決済金額
    charges.amount,

    -- from fixed_crm_customers
    ---- 姓
    crm_customers.first_name,
    ---- 名
    crm_customers.last_name,
    ---- フルネーム
    crm_customers.full_name,
    ---- 暗号化済みEメール
    crm_customers.hashed_email
FROM
    shopify_orders
LEFT OUTER JOIN
    charges
ON
    shopify_orders.payment_id = charges.id
LEFT OUTER JOIN
    crm_customers
ON
    shopify_orders.customer_id = crm_customers.id