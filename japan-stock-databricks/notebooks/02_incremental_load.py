# Databricks notebook source
# MAGIC %md
# MAGIC # 02 増分ロード: 前回ロード日の翌日〜昨日を取得して MERGE
# MAGIC
# MAGIC Databricks Workflow から日次で呼び出すことを想定しています。

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo-path>/japan-stock-databricks")

from datetime import datetime
from src.common.utils import get_logger, get_yesterday
from src.ingestion.jquants_client import JQuantsClient
from src.bronze.load_bronze import BronzeLoader
from src.silver.build_silver import SilverBuilder
from src.quality.data_quality import DataQualityChecker
from src.gold.build_gold import GoldBuilder

logger = get_logger("02_incremental_load")

try:
    email    = dbutils.secrets.get(scope="jquants", key="email")
    password = dbutils.secrets.get(scope="jquants", key="password")
except Exception:
    import os
    email    = os.environ["JQUANTS_EMAIL"]
    password = os.environ["JQUANTS_PASSWORD"]

# COMMAND ----------
# MAGIC %md ## 1. 前回最終ロード日を確認

# COMMAND ----------
def get_last_loaded_date(spark) -> str:
    """Bronze テーブルから最後に取り込まれた Date を取得"""
    try:
        row = spark.sql(
            "SELECT MAX(Date) as max_date FROM main.bronze_stocks.daily_quotes"
        ).first()
        return row["max_date"] if row["max_date"] else None
    except Exception:
        return None

last_date = get_last_loaded_date(spark)
date_to   = get_yesterday()

if last_date is None:
    raise Exception("Bronze table not found. Run 01_initial_load first.")

# 翌日から昨日まで
from datetime import timedelta
last_dt   = datetime.strptime(last_date, "%Y-%m-%d")
date_from = (last_dt + timedelta(days=1)).strftime("%Y-%m-%d")

logger.info(f"Incremental load: {date_from} → {date_to}")

if date_from > date_to:
    logger.info("Already up to date. Nothing to load.")
    dbutils.notebook.exit("up_to_date")

# COMMAND ----------
# MAGIC %md ## 2. 株価データ取得 → Bronze

# COMMAND ----------
client = JQuantsClient(email=email, password=password)
bronze_loader = BronzeLoader(spark)
RAW_PATH = "/dbfs/raw/japan_stocks/prices"

quotes = client.get_prices_daily_quotes(date_from=date_from, date_to=date_to)
logger.info(f"Fetched {len(quotes)} records from API")

if quotes:
    partition_key = date_from.replace("-", "")
    raw_file = client.save_raw_json(quotes, "prices", partition_key, RAW_PATH)
    n = bronze_loader.load_prices_from_json(raw_file)
    logger.info(f"Bronze MERGE: {n} records")
else:
    logger.warning("No price data returned from API (holiday or market closed)")

# COMMAND ----------
# MAGIC %md ## 3. 銘柄情報更新（週次相当 — 月曜のみ更新）

# COMMAND ----------
today = datetime.now()
if today.weekday() == 0:  # 月曜
    listed = client.get_listed_info()
    bronze_loader.load_listed_info(listed)
    logger.info(f"Listed info refreshed: {len(listed)} stocks")

# COMMAND ----------
# MAGIC %md ## 4. Silver MERGE（増分のみ）

# COMMAND ----------
silver_builder = SilverBuilder(spark)
silver_builder.build_daily_quotes(date_from=date_from, date_to=date_to)
silver_builder.build_listed_info()

# COMMAND ----------
# MAGIC %md ## 5. Data Quality チェック

# COMMAND ----------
dq = DataQualityChecker(spark)
report = dq.check_silver_daily_quotes(date_from=date_from, date_to=date_to, fail_on_error=True)
print(report.summary())
dq.save_dq_report(report)

# COMMAND ----------
# MAGIC %md ## 6. Gold 再構築

# COMMAND ----------
gold_builder = GoldBuilder(spark)
gold_builder.build_all()

logger.info("=== Incremental Load Complete ===")
print(f"✅ Incremental load done: {date_from} → {date_to}  ({len(quotes)} records)")
