# Databricks notebook source
# MAGIC %md
# MAGIC # 01 初回ロード: J-Quants API → Bronze → Silver → Gold
# MAGIC
# MAGIC **前提**
# MAGIC - Databricks Secrets: `jquants/email`, `jquants/password` を登録済み
# MAGIC - ライブラリ: `requirements.txt` を Cluster にインストール済み

# COMMAND ----------
# MAGIC %md ## 0. 設定

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo-path>/japan-stock-databricks")

from datetime import datetime, timedelta
from src.common.utils import get_logger, get_date_n_years_ago, get_yesterday
from src.ingestion.jquants_client import JQuantsClient
from src.bronze.load_bronze import BronzeLoader
from src.silver.build_silver import SilverBuilder
from src.quality.data_quality import DataQualityChecker
from src.gold.build_gold import GoldBuilder

logger = get_logger("01_initial_load")

# Databricks Secrets から idToken（アクセストークン）取得
try:
    id_token = dbutils.secrets.get(scope="jquants", key="id_token")
except Exception:
    import os
    id_token = os.environ["JQUANTS_ID_TOKEN"]

DATE_FROM = get_date_n_years_ago(3)   # 3年前
DATE_TO   = get_yesterday()

logger.info(f"Initial load: {DATE_FROM} → {DATE_TO}")

# COMMAND ----------
# MAGIC %md ## 1. J-Quants API クライアント初期化

# COMMAND ----------
client = JQuantsClient(id_token=id_token)

# COMMAND ----------
# MAGIC %md ## 2. 上場銘柄情報取得 → Bronze

# COMMAND ----------
logger.info("Fetching listed info ...")
listed_info = client.get_listed_info()
logger.info(f"  {len(listed_info)} stocks fetched")

bronze_loader = BronzeLoader(spark)
bronze_loader.load_listed_info(listed_info)
logger.info("Listed info → Bronze: done")

# COMMAND ----------
# MAGIC %md ## 3. 日次株価データ取得 → Bronze (月次バッチ)
# MAGIC
# MAGIC J-Quants Free プランは1リクエストで取得できる日付範囲に制限があるため、
# MAGIC 月単位でループして取得します。

# COMMAND ----------
import json, os
from dateutil.relativedelta import relativedelta

RAW_PATH = "/dbfs/raw/japan_stocks/prices"

def month_ranges(start: str, end: str):
    """開始日〜終了日を月単位に分割"""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    cur = s.replace(day=1)
    while cur <= e:
        nxt = cur + relativedelta(months=1)
        yield (
            max(cur, s).strftime("%Y-%m-%d"),
            min(nxt - timedelta(days=1), e).strftime("%Y-%m-%d"),
        )
        cur = nxt

total_records = 0
for mfrom, mto in month_ranges(DATE_FROM, DATE_TO):
    logger.info(f"Fetching prices {mfrom} → {mto}")
    try:
        quotes = client.get_prices_daily_quotes(date_from=mfrom, date_to=mto)
        if not quotes:
            logger.info(f"  No data for {mfrom}~{mto}")
            continue

        # Raw JSON 保存
        partition_key = mfrom[:7].replace("-", "")  # YYYYMM
        raw_file = client.save_raw_json(quotes, "prices", partition_key, RAW_PATH)

        # Bronze MERGE
        n = bronze_loader.load_prices_from_json(raw_file, ingestion_date=datetime.now().strftime("%Y-%m-%d"))
        total_records += n
        logger.info(f"  Merged {n} records → Bronze")
    except Exception as ex:
        logger.error(f"  Failed {mfrom}~{mto}: {ex}")

logger.info(f"Bronze total records: {total_records}")

# COMMAND ----------
# MAGIC %md ## 4. Bronze OPTIMIZE

# COMMAND ----------
bronze_loader.optimize_table("daily_quotes", zorder_cols=["Code"])

# COMMAND ----------
# MAGIC %md ## 5. Silver 変換

# COMMAND ----------
silver_builder = SilverBuilder(spark)
n_quotes  = silver_builder.build_daily_quotes(date_from=DATE_FROM, date_to=DATE_TO)
n_listing = silver_builder.build_listed_info()
logger.info(f"Silver: {n_quotes} quotes, {n_listing} listed_info")

# COMMAND ----------
# MAGIC %md ## 6. Data Quality チェック

# COMMAND ----------
dq = DataQualityChecker(spark)
report = dq.check_silver_daily_quotes(date_from=DATE_FROM, date_to=DATE_TO)
print(report.summary())
dq.save_dq_report(report)

if not report.passed:
    raise Exception("DQ checks failed — please review the report above.")

# COMMAND ----------
# MAGIC %md ## 7. Gold 構築

# COMMAND ----------
gold_builder = GoldBuilder(spark)
gold_builder.build_all()

logger.info("=== Initial Load Complete ===")
print(f"""
✅ Initial load finished!
   Period   : {DATE_FROM} → {DATE_TO}
   Records  : {total_records}
   Bronze   : main.bronze_stocks.daily_quotes
   Silver   : main.silver_stocks.daily_quotes
   Gold     : main.gold_stocks.stock_with_master
""")
