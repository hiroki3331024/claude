"""Gold レイヤー: 分析用集計・指標計算"""
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from src.common.utils import get_logger, load_config

logger = get_logger(__name__)


class GoldBuilder:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        cfg = load_config()
        self.catalog = cfg["databricks"]["catalog"]
        self.silver_schema = cfg["databricks"]["schema_silver"]
        self.gold_schema = cfg["databricks"]["schema_gold"]
        self.ma_windows = cfg["gold"]["moving_average_windows"]      # [5, 25, 75]
        self.vol_window = cfg["gold"]["volatility_window"]            # 20
        self.return_windows = cfg["gold"]["return_windows"]           # [1, 5, 21]

    def _full(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    def _ensure_schema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.gold_schema}")

    # ── テクニカル指標付き株価テーブル ────────────────────────────────────

    def build_stock_metrics(self) -> int:
        """移動平均・リターン・ボラティリティを計算した Gold テーブルを構築"""
        self._ensure_schema()

        df = (
            self.spark.table(self._full(self.silver_schema, "daily_quotes"))
            .select("trade_date", "code", "open", "high", "low", "close", "volume", "turnover_value")
        )

        w_code = Window.partitionBy("code").orderBy("trade_date")
        w_vol = Window.partitionBy("code").orderBy("trade_date").rowsBetween(-(self.vol_window - 1), 0)

        # 移動平均
        for days in self.ma_windows:
            w_ma = Window.partitionBy("code").orderBy("trade_date").rowsBetween(-(days - 1), 0)
            df = df.withColumn(f"ma{days}", F.avg("close").over(w_ma))

        # リターン (前日比 %)
        for days in self.return_windows:
            df = df.withColumn(
                f"return_{days}d",
                (F.col("close") / F.lag("close", days).over(w_code) - 1) * 100
            )

        # ボラティリティ (標準偏差 of 日次リターン)
        df = df.withColumn(
            "_daily_ret",
            (F.col("close") / F.lag("close", 1).over(w_code) - 1) * 100
        )
        df = df.withColumn(f"volatility_{self.vol_window}d", F.stddev("_daily_ret").over(w_vol))
        df = df.drop("_daily_ret")

        # 出来高移動平均 (5日)
        w_vol5 = Window.partitionBy("code").orderBy("trade_date").rowsBetween(-4, 0)
        df = df.withColumn("volume_ma5", F.avg("volume").over(w_vol5))

        # 高値・安値 乖離 (52週高値・安値からの距離)
        w_52w = Window.partitionBy("code").orderBy("trade_date").rowsBetween(-251, 0)
        df = df.withColumn("high_52w", F.max("high").over(w_52w))
        df = df.withColumn("low_52w", F.min("low").over(w_52w))
        df = df.withColumn("dist_from_high_52w", (F.col("close") / F.col("high_52w") - 1) * 100)
        df = df.withColumn("dist_from_low_52w", (F.col("close") / F.col("low_52w") - 1) * 100)

        df = df.withColumn("_gold_updated_at", F.current_timestamp())

        count = df.count()
        table_name = self._full(self.gold_schema, "stock_metrics")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("trade_date")
            .saveAsTable(table_name)
        )

        logger.info(f"Gold stock_metrics: {count} records")
        return count

    # ── 銘柄マスタ結合ビュー ─────────────────────────────────────────────

    def build_stock_master_joined(self) -> int:
        """株価指標に銘柄マスタを JOIN した Gold テーブル"""
        self._ensure_schema()

        metrics = self.spark.table(self._full(self.gold_schema, "stock_metrics"))
        listed = self.spark.table(self._full(self.silver_schema, "listed_info"))

        df = metrics.join(
            listed.select("code", "company_name", "sector17_name", "sector33_name", "market_name", "scale_category"),
            on="code",
            how="left"
        ).withColumn("_gold_updated_at", F.current_timestamp())

        count = df.count()
        table_name = self._full(self.gold_schema, "stock_with_master")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("trade_date")
            .saveAsTable(table_name)
        )

        logger.info(f"Gold stock_with_master: {count} records")
        return count

    # ── セクター集計 ──────────────────────────────────────────────────────

    def build_sector_summary(self) -> int:
        """セクター別・日次集計"""
        self._ensure_schema()

        df = self.spark.table(self._full(self.gold_schema, "stock_with_master"))

        sector_df = (
            df.groupBy("trade_date", "sector17_name")
            .agg(
                F.count("code").alias("stock_count"),
                F.avg("return_1d").alias("avg_return_1d"),
                F.avg("return_5d").alias("avg_return_5d"),
                F.sum("turnover_value").alias("total_turnover"),
                F.avg("volatility_20d").alias("avg_volatility"),
            )
            .withColumn("_gold_updated_at", F.current_timestamp())
        )

        count = sector_df.count()
        table_name = self._full(self.gold_schema, "sector_summary")
        (
            sector_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("trade_date")
            .saveAsTable(table_name)
        )

        logger.info(f"Gold sector_summary: {count} records")
        return count

    def build_all(self):
        """全 Gold テーブルを順に構築"""
        logger.info("Building Gold: stock_metrics ...")
        self.build_stock_metrics()
        logger.info("Building Gold: stock_with_master ...")
        self.build_stock_master_joined()
        logger.info("Building Gold: sector_summary ...")
        self.build_sector_summary()
        logger.info("All Gold tables built.")
