# Databricks notebook source
# MAGIC %md
# MAGIC # 03 パイプライン実行管理
# MAGIC
# MAGIC 初回ロードか増分ロードかを自動判定して適切なNotebookを呼び出します。
# MAGIC Workflow の最初のTaskとして設定することを推奨します。

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo-path>/japan-stock-databricks")

from src.common.utils import get_logger

logger = get_logger("03_run_pipeline")

REPO_BASE = "/Workspace/Repos/<your-repo-path>/japan-stock-databricks"

# COMMAND ----------
# MAGIC %md ## Bronze テーブルの存在確認

# COMMAND ----------
def bronze_exists(spark) -> bool:
    try:
        spark.table("main.bronze_stocks.daily_quotes")
        return True
    except Exception:
        return False

if bronze_exists(spark):
    logger.info("Bronze table found → Running incremental load")
    target_notebook = f"{REPO_BASE}/notebooks/02_incremental_load"
else:
    logger.info("Bronze table not found → Running initial load")
    target_notebook = f"{REPO_BASE}/notebooks/01_initial_load"

# COMMAND ----------
result = dbutils.notebook.run(target_notebook, timeout_seconds=7200)
logger.info(f"Pipeline result: {result}")
print(f"Pipeline finished: {result}")
