-- ============================================================
-- スキーマ作成
-- ============================================================
-- Databricks Unity Catalog を使用
-- Catalog: main (Free Edition デフォルト)

CREATE SCHEMA IF NOT EXISTS main.bronze_stocks
COMMENT 'Raw ingestion layer for Japan stock data from J-Quants API';

CREATE SCHEMA IF NOT EXISTS main.silver_stocks
COMMENT 'Cleansed and normalized Japan stock data';

CREATE SCHEMA IF NOT EXISTS main.gold_stocks
COMMENT 'Aggregated and enriched Japan stock data for analytics and dashboards';

-- ============================================================
-- Bronze テーブル確認クエリ
-- ============================================================
SHOW TABLES IN main.bronze_stocks;

-- 最新取り込み日確認
SELECT
  MAX(Date) AS last_loaded_date,
  COUNT(DISTINCT Date) AS loaded_days,
  COUNT(DISTINCT Code) AS stock_count,
  COUNT(*) AS total_records
FROM main.bronze_stocks.daily_quotes;

-- ============================================================
-- Silver テーブル確認クエリ
-- ============================================================
SHOW TABLES IN main.silver_stocks;

SELECT
  MAX(trade_date) AS last_date,
  COUNT(DISTINCT trade_date) AS trading_days,
  COUNT(DISTINCT code) AS stock_count,
  COUNT(*) AS total_records
FROM main.silver_stocks.daily_quotes;

-- DQ ログ確認
SELECT * FROM main.silver_stocks.dq_log
ORDER BY checked_at DESC
LIMIT 20;

-- ============================================================
-- Gold テーブル確認クエリ
-- ============================================================
SHOW TABLES IN main.gold_stocks;

-- 最新日の主要指標サンプル
SELECT
  trade_date,
  code,
  company_name,
  sector17_name,
  close,
  return_1d,
  return_5d,
  ma5,
  ma25,
  volatility_20d
FROM main.gold_stocks.stock_with_master
WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master)
ORDER BY turnover_value DESC
LIMIT 30;
