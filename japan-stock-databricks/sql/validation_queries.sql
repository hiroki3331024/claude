-- ============================================================
-- 検証・モニタリングクエリ集
-- ============================================================

-- 1. 日次取り込みレコード数の推移
SELECT
  Date AS trade_date,
  COUNT(*) AS record_count,
  COUNT(DISTINCT Code) AS stock_count
FROM main.bronze_stocks.daily_quotes
GROUP BY Date
ORDER BY Date DESC
LIMIT 30;

-- 2. 欠損日確認 (直近30営業日)
WITH date_series AS (
  SELECT explode(sequence(
    date_add(current_date(), -45),
    date_add(current_date(), -1),
    INTERVAL 1 DAY
  )) AS cal_date
),
trading_dates AS (
  SELECT DISTINCT CAST(Date AS DATE) AS trade_date
  FROM main.bronze_stocks.daily_quotes
)
SELECT
  cal_date,
  CASE
    WHEN dayofweek(cal_date) IN (1, 7) THEN 'Weekend'
    WHEN trade_date IS NULL THEN '⚠️ Missing (possible holiday)'
    ELSE 'OK'
  END AS status
FROM date_series
LEFT JOIN trading_dates ON cal_date = trade_date
ORDER BY cal_date DESC;

-- 3. 銘柄別 最終価格・移動平均乖離率
SELECT
  code,
  company_name,
  sector17_name,
  close,
  ma25,
  ROUND((close / ma25 - 1) * 100, 2) AS pct_above_ma25,
  return_1d,
  return_5d,
  volatility_20d,
  volume
FROM main.gold_stocks.stock_with_master
WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master)
ORDER BY ABS(return_1d) DESC
LIMIT 50;

-- 4. セクター別パフォーマンス (直近5営業日平均)
WITH recent AS (
  SELECT *
  FROM main.gold_stocks.sector_summary
  WHERE trade_date >= date_add(
    (SELECT MAX(trade_date) FROM main.gold_stocks.sector_summary), -7
  )
)
SELECT
  sector17_name,
  ROUND(AVG(avg_return_1d), 3) AS avg_daily_return,
  ROUND(AVG(avg_return_5d), 3) AS avg_5d_return,
  ROUND(SUM(total_turnover) / 1e9, 1) AS total_turnover_b_jpy,
  ROUND(AVG(avg_volatility), 3) AS avg_volatility
FROM recent
GROUP BY sector17_name
ORDER BY avg_daily_return DESC;

-- 5. リターン上位・下位 (本日)
(
  SELECT 'TOP' AS rank_type, code, company_name, close, return_1d, volume
  FROM main.gold_stocks.stock_with_master
  WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master)
    AND return_1d IS NOT NULL
  ORDER BY return_1d DESC
  LIMIT 10
)
UNION ALL
(
  SELECT 'BOTTOM' AS rank_type, code, company_name, close, return_1d, volume
  FROM main.gold_stocks.stock_with_master
  WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master)
    AND return_1d IS NOT NULL
  ORDER BY return_1d ASC
  LIMIT 10
);

-- 6. データ品質サマリー
SELECT
  table_name,
  check_name,
  CASE WHEN passed THEN '✅ PASS' ELSE '❌ FAIL' END AS status,
  detail,
  checked_at
FROM main.silver_stocks.dq_log
WHERE checked_at >= date_add(current_timestamp(), -1)
ORDER BY checked_at DESC, table_name, check_name;
