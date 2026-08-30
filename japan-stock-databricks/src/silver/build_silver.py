"""Silver レイヤー: Bronze → クレンジング・正規化済みデータ"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from src.common.utils import get_logger, load_config

logger = get_logger(__name__)


class SilverBuilder:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        cfg = load_config()
        self.catalog = cfg["databricks"]["catalog"]
        self.bronze_schema = cfg["databricks"]["schema_bronze"]
        self.silver_schema = cfg["databricks"]["schema_silver"]

    def _full(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    def _ensure_schema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.silver_schema}")

    def build_daily_quotes(self, date_from: str = None, date_to: str = None) -> int:
        """Bronze の daily_quotes を Silver へ変換"""
        self._ensure_schema()

        src = self.spark.table(self._full(self.bronze_schema, "daily_quotes"))

        if date_from:
            src = src.filter(F.col("Date") >= date_from)
        if date_to:
            src = src.filter(F.col("Date") <= date_to)

        silver_df = (
            src
            # 型変換
            .withColumn("trade_date", F.to_date("Date", "yyyy-MM-dd"))
            .withColumn("code", F.col("Code").cast("string"))
            # 欠損・異常値フィルタ: Close が null または 0 以下は除外
            .filter(F.col("AdjustmentClose").isNotNull() & (F.col("AdjustmentClose") > 0))
            .filter(F.col("AdjustmentVolume").isNotNull() & (F.col("AdjustmentVolume") >= 0))
            # カラム選択・リネーム
            .select(
                "trade_date",
                "code",
                F.col("AdjustmentOpen").alias("open"),
                F.col("AdjustmentHigh").alias("high"),
                F.col("AdjustmentLow").alias("low"),
                F.col("AdjustmentClose").alias("close"),
                F.col("AdjustmentVolume").alias("volume"),
                F.col("TurnoverValue").alias("turnover_value"),
                F.col("AdjustmentFactor").alias("adjustment_factor"),
                # 原値も残す
                F.col("Open").alias("raw_open"),
                F.col("High").alias("raw_high"),
                F.col("Low").alias("raw_low"),
                F.col("Close").alias("raw_close"),
                F.col("Volume").alias("raw_volume"),
                "_ingestion_date",
                F.current_timestamp().alias("_silver_updated_at"),
            )
        )

        count = silver_df.count()
        table_name = self._full(self.silver_schema, "daily_quotes")

        if not self._table_exists(table_name):
            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("trade_date")
                .saveAsTable(table_name)
            )
        else:
            dt = DeltaTable.forName(self.spark, table_name)
            (
                dt.alias("tgt")
                .merge(
                    silver_df.alias("src"),
                    "tgt.trade_date = src.trade_date AND tgt.code = src.code"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        logger.info(f"Silver daily_quotes: {count} records upserted")
        return count

    def build_listed_info(self) -> int:
        """Bronze の listed_info を Silver へコピー（フィルタ・正規化）"""
        self._ensure_schema()

        src = self.spark.table(self._full(self.bronze_schema, "listed_info"))

        silver_df = (
            src
            .filter(F.col("Code").isNotNull())
            .select(
                F.col("Code").alias("code"),
                F.col("CompanyName").alias("company_name"),
                F.col("CompanyNameEnglish").alias("company_name_en"),
                F.col("Sector17Code").alias("sector17_code"),
                F.col("Sector17CodeName").alias("sector17_name"),
                F.col("Sector33Code").alias("sector33_code"),
                F.col("Sector33CodeName").alias("sector33_name"),
                F.col("ScaleCategory").alias("scale_category"),
                F.col("MarketCode").alias("market_code"),
                F.col("MarketCodeName").alias("market_name"),
                "_ingestion_date",
                F.current_timestamp().alias("_silver_updated_at"),
            )
            .dropDuplicates(["code"])
        )

        count = silver_df.count()
        table_name = self._full(self.silver_schema, "listed_info")

        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        logger.info(f"Silver listed_info: {count} records")
        return count

    def _table_exists(self, full_table_name: str) -> bool:
        try:
            self.spark.table(full_table_name)
            return True
        except Exception:
            return False
