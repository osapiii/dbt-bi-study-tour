
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

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
    {{ ref('ex3_raw_crm_customers') }}