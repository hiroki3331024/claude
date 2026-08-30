"""Bronze レイヤー: Raw JSON → Delta Table"""
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType
)
from delta.tables import DeltaTable

from src.common.utils import get_logger, load_config

logger = get_logger(__name__)


PRICE_SCHEMA = StructType([
    StructField("Date", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("TurnoverValue", DoubleType(), True),
    StructField("AdjustmentFactor", DoubleType(), True),
    StructField("AdjustmentOpen", DoubleType(), True),
    StructField("AdjustmentHigh", DoubleType(), True),
    StructField("AdjustmentLow", DoubleType(), True),
    StructField("AdjustmentClose", DoubleType(), True),
    StructField("AdjustmentVolume", DoubleType(), True),
])

LISTED_INFO_SCHEMA = StructType([
    StructField("Date", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("CompanyName", StringType(), True),
    StructField("CompanyNameEnglish", StringType(), True),
    StructField("Sector17Code", StringType(), True),
    StructField("Sector17CodeName", StringType(), True),
    StructField("Sector33Code", StringType(), True),
    StructField("Sector33CodeName", StringType(), True),
    StructField("ScaleCategory", StringType(), True),
    StructField("MarketCode", StringType(), True),
    StructField("MarketCodeName", StringType(), True),
])


class BronzeLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        cfg = load_config()
        self.catalog = cfg["databricks"]["catalog"]
        self.schema = cfg["databricks"]["schema_bronze"]
        self.raw_path = cfg["databricks"]["raw_data_path"]

    def _full_table(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    def _ensure_schema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

    # ── 株価 Bronze ────────────────────────────────────────────────────────

    def load_prices_from_json(self, json_path: str, ingestion_date: Optional[str] = None) -> int:
        """Raw JSONファイルを Bronze Delta Table に MERGE する"""
        self._ensure_schema()
        if ingestion_date is None:
            ingestion_date = datetime.now().strftime("%Y-%m-%d")

        df = (
            self.spark.read.schema(PRICE_SCHEMA).json(json_path)
            .withColumn("_ingestion_date", F.lit(ingestion_date))
            .withColumn("_source_file", F.lit(json_path))
            .withColumn("_loaded_at", F.current_timestamp())
        )

        count = df.count()
        logger.info(f"Loaded {count} records from {json_path}")

        table_name = self._full_table("daily_quotes")
        self._merge_prices(df, table_name)
        return count

    def _merge_prices(self, df: DataFrame, table_name: str):
        if not self._table_exists(table_name):
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("Date")
                .saveAsTable(table_name)
            )
            logger.info(f"Created Bronze table: {table_name}")
            return

        dt = DeltaTable.forName(self.spark, table_name)
        (
            dt.alias("tgt")
            .merge(df.alias("src"), "tgt.Date = src.Date AND tgt.Code = src.Code")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"MERGE complete into {table_name}")

    # ── 銘柄情報 Bronze ───────────────────────────────────────────────────

    def load_listed_info(self, data: list[dict], ingestion_date: Optional[str] = None) -> int:
        self._ensure_schema()
        if ingestion_date is None:
            ingestion_date = datetime.now().strftime("%Y-%m-%d")

        df = (
            self.spark.createDataFrame(data, schema=LISTED_INFO_SCHEMA)
            .withColumn("_ingestion_date", F.lit(ingestion_date))
            .withColumn("_loaded_at", F.current_timestamp())
        )

        count = df.count()
        table_name = self._full_table("listed_info")

        if not self._table_exists(table_name):
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        else:
            dt = DeltaTable.forName(self.spark, table_name)
            (
                dt.alias("tgt")
                .merge(df.alias("src"), "tgt.Code = src.Code")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        logger.info(f"Loaded {count} listed_info records")
        return count

    def _table_exists(self, full_table_name: str) -> bool:
        try:
            self.spark.table(full_table_name)
            return True
        except Exception:
            return False

    def optimize_table(self, table: str, zorder_cols: list[str] = None):
        """OPTIMIZE + ZORDER でクエリ性能を向上"""
        full = self._full_table(table)
        zorder = f"ZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""
        self.spark.sql(f"OPTIMIZE {full} {zorder}")
        logger.info(f"OPTIMIZE complete: {full}")
