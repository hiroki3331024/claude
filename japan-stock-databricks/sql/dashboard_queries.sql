-- ============================================================
-- Dashboard 用クエリ集
-- Databricks SQL Dashboard / Genie で利用
-- ============================================================

-- ▼ Widget 1: マーケットサマリー (KPI カード)
SELECT
  (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master) AS latest_date,
  COUNT(DISTINCT code)                                              AS total_stocks,
  ROUND(AVG(return_1d), 3)                                         AS market_avg_return_1d,
  COUNT(CASE WHEN return_1d > 0 THEN 1 END)                        AS advancing_stocks,
  COUNT(CASE WHEN return_1d < 0 THEN 1 END)                        AS declining_stocks,
  ROUND(SUM(turnover_value) / 1e12, 2)                             AS total_turnover_t_jpy
FROM main.gold_stocks.stock_with_master
WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master);


-- ▼ Widget 2: セクター別リターン (棒グラフ)
SELECT
  sector17_name,
  ROUND(avg_return_1d, 3)  AS return_1d,
  ROUND(avg_return_5d, 3)  AS return_5d,
  ROUND(total_turnover / 1e9, 1) AS turnover_b_jpy
FROM main.gold_stocks.sector_summary
WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.sector_summary)
ORDER BY return_1d DESC;


-- ▼ Widget 3: 個別銘柄 時系列 (折れ線グラフ)
-- パラメーター: {{stock_code}} (例: 7203 = トヨタ)
SELECT
  trade_date,
  code,
  company_name,
  close,
  ma5,
  ma25,
  ma75,
  volume
FROM main.gold_stocks.stock_with_master
WHERE code = '{{stock_code}}'
  AND trade_date >= date_add(current_date(), -365)
ORDER BY trade_date;


-- ▼ Widget 4: 騰落率ランキング TOP20 (テーブル)
SELECT
  ROW_NUMBER() OVER (ORDER BY return_1d DESC) AS rank,
  code,
  company_name,
  sector17_name,
  ROUND(close, 0) AS close,
  ROUND(return_1d, 2) AS return_1d_pct,
  ROUND(volume / 1e6, 1) AS volume_m,
  ROUND(volatility_20d, 2) AS volatility_20d
FROM main.gold_stocks.stock_with_master
WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master)
  AND return_1d IS NOT NULL
ORDER BY return_1d DESC
LIMIT 20;


-- ▼ Widget 5: ボラティリティ vs リターン (散布図)
SELECT
  code,
  company_name,
  sector17_name,
  ROUND(return_5d, 2)       AS return_5d,
  ROUND(volatility_20d, 2)  AS volatility_20d,
  ROUND(turnover_value / 1e6, 1) AS turnover_m_jpy
FROM main.gold_stocks.stock_with_master
WHERE trade_date = (SELECT MAX(trade_date) FROM main.gold_stocks.stock_with_master)
  AND return_5d IS NOT NULL
  AND volatility_20d IS NOT NULL
  AND turnover_value > 1e8  -- 出来高フィルタ
ORDER BY turnover_value DESC
LIMIT 500;


-- ▼ Widget 6: 市場別 出来高推移 (折れ線)
SELECT
  trade_date,
  market_name,
  ROUND(SUM(turnover_value) / 1e12, 3) AS total_turnover_t_jpy
FROM main.gold_stocks.stock_with_master
WHERE trade_date >= date_add(current_date(), -90)
GROUP BY trade_date, market_name
ORDER BY trade_date, market_name;


-- ▼ Genie 用: 自然言語質問に回答しやすい統合ビュー
-- このクエリを Genie のデータセットとして登録してください
SELECT
  s.trade_date,
  s.code,
  s.company_name,
  s.sector17_name   AS sector,
  s.market_name     AS market,
  s.scale_category,
  s.close,
  s.open,
  s.high,
  s.low,
  s.volume,
  s.turnover_value,
  s.ma5,
  s.ma25,
  s.ma75,
  s.return_1d,
  s.return_5d,
  s.return_21d,
  s.volatility_20d,
  s.high_52w,
  s.low_52w,
  s.dist_from_high_52w,
  s.dist_from_low_52w,
  s.volume_ma5
FROM main.gold_stocks.stock_with_master s
WHERE s.trade_date >= date_add(current_date(), -730)  -- 直近2年
