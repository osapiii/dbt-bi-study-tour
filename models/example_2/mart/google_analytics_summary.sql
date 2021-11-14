{{ config(materialized='view') }}

SELECT
    -- from ex3_fixed_google_analytics
    ex3_fixed_google_analytics.visitorId,
    ex3_fixed_google_analytics.visitNumber,
    ex3_fixed_google_analytics.visitId,
    ex3_fixed_google_analytics.visitStartTime,
    ex3_fixed_google_analytics.date,
    ex3_fixed_google_analytics.totals,
    ex3_fixed_google_analytics.trafficSource,
    ex3_fixed_google_analytics.device,
    ex3_fixed_google_analytics.geoNetwork,
    ex3_fixed_google_analytics.customDimensions,
    ex3_fixed_google_analytics.region,
    ex3_fixed_google_analytics.hits,
    ex3_fixed_google_analytics.fullVisitorId,
    ex3_fixed_google_analytics.userId,
    ex3_fixed_google_analytics.clientId,
    ex3_fixed_google_analytics.channelGrouping,
    ex3_fixed_google_analytics.socialEngagementType,

    -- from fixed_crm_customers
    ---- 姓
    ex3_fixed_crm_customers.first_name,
    ---- 名
    ex3_fixed_crm_customers.last_name,
    ---- フルネーム
    ex3_fixed_crm_customers.full_name,
    ---- 暗号化済みEメール
    ex3_fixed_crm_customers.hashed_email
FROM
    {{ ref('fixed_google_analytics') }} AS ex3_fixed_google_analytics
LEFT OUTER JOIN
    {{ ref('fixed_crm_customers') }} AS ex3_fixed_crm_customers
ON
    ex3_fixed_google_analytics.clientId = ex3_fixed_crm_customers.id