{{ config(materialized='view') }}
-- 全件取得
SELECT
    *
FROM
  dataform-demos.dataform_tutorial.crm_customers AS customers