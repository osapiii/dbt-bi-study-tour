{{ config(materialized='view') }}

SELECT
    visitorId,
    visitNumber,
    visitId,
    visitStartTime,
    date,
    totals,
    trafficSource,
    device,
    geoNetwork,
    customDimensions,
    -- 【典型前処理① 配列の展開】GA配列の展開
    (SELECT value FROM unnest(customDimensions) WHERE index = 4) AS region,
    hits,
    fullVisitorId,
    userId,
    -- 【典型前処理① 型変換】JOIN用にclientIdを文字列型->整数型に変更
    CAST(clientId AS INT64) AS clientId,
    channelGrouping,
    socialEngagementType
FROM
    {{ ref('raw_google_analytics') }}
WHERE
    -- 【典型前処理① 期間絞り込み】古いセッションログの除外
    date > '2015-04-01'