"""変換ロジックのユニットテスト (pytest + pyspark)"""
import pytest
from datetime import date
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_japan_stocks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# ── JQuantsClient ─────────────────────────────────────────────────────────

class TestJQuantsClient:
    def test_date_format_normalization(self):
        """ハイフン付き日付を YYYYMMDD 形式に変換することを確認"""
        from src.ingestion.jquants_client import JQuantsClient
        with patch.object(JQuantsClient, "_get", return_value={"daily_quotes": []}) as mock_get:
            client = JQuantsClient(email="test@example.com", password="pass")
            client._id_token = "dummy"
            client._token_expires_at = float("inf")
            client.get_prices_daily_quotes(date_from="2024-01-01", date_to="2024-01-31")
            _, kwargs = mock_get.call_args
            params = kwargs.get("params", mock_get.call_args[0][1] if len(mock_get.call_args[0]) > 1 else {})
            assert params.get("from") == "20240101"
            assert params.get("to") == "20240131"


# ── Common Utils ──────────────────────────────────────────────────────────

class TestUtils:
    def test_date_range(self):
        from src.common.utils import date_range
        r = date_range("2024-01-01", "2024-01-05")
        assert r == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]

    def test_date_range_single(self):
        from src.common.utils import date_range
        r = date_range("2024-03-15", "2024-03-15")
        assert r == ["2024-03-15"]

    def test_resolve_env_vars(self):
        import os
        from src.common.utils import _resolve_env_vars
        os.environ["TEST_VAR_XYZ"] = "hello"
        assert _resolve_env_vars("${TEST_VAR_XYZ}") == "hello"
        assert _resolve_env_vars("plain") == "plain"
        assert _resolve_env_vars({"k": "${TEST_VAR_XYZ}"}) == {"k": "hello"}


# ── Silver 変換 ────────────────────────────────────────────────────────────

class TestSilverTransformation:
    def _make_bronze_df(self, spark):
        data = [
            ("2024-01-05", "7203", 2500.0, 2520.0, 2490.0, 2510.0, 100000.0, 250000000.0, 1.0,
             2500.0, 2520.0, 2490.0, 2510.0, 100000.0),
            ("2024-01-05", "9984", 7000.0, 7100.0, 6950.0, 7050.0, 50000.0, 350000000.0, 1.0,
             7000.0, 7100.0, 6950.0, 7050.0, 50000.0),
            # AdjustmentClose が null → Silver で除外されるはず
            ("2024-01-05", "0000", None, None, None, None, None, None, None,
             None, None, None, None, None),
        ]
        cols = [
            "Date", "Code", "Open", "High", "Low", "Close",
            "Volume", "TurnoverValue", "AdjustmentFactor",
            "AdjustmentOpen", "AdjustmentHigh", "AdjustmentLow",
            "AdjustmentClose", "AdjustmentVolume",
        ]
        return spark.createDataFrame(data, cols)

    def test_null_close_filtered(self, spark):
        """AdjustmentClose が null の行は Silver で除外される"""
        df = self._make_bronze_df(spark)
        silver = (
            df
            .filter(F.col("AdjustmentClose").isNotNull() & (F.col("AdjustmentClose") > 0))
        )
        assert silver.count() == 2

    def test_column_rename(self, spark):
        """カラムリネームが正しく行われる"""
        df = self._make_bronze_df(spark)
        silver = (
            df
            .filter(F.col("AdjustmentClose").isNotNull())
            .withColumn("close", F.col("AdjustmentClose"))
            .withColumn("trade_date", F.to_date("Date", "yyyy-MM-dd"))
        )
        assert "close" in silver.columns
        assert "trade_date" in silver.columns
        row = silver.filter(F.col("Code") == "7203").first()
        assert row["close"] == 2510.0


# ── Gold 指標計算 ──────────────────────────────────────────────────────────

class TestGoldMetrics:
    def _make_silver_df(self, spark):
        data = [
            (date(2024, 1, 5), "7203", 2500.0, 2520.0, 2490.0, 2500.0, 100000.0, 1e8),
            (date(2024, 1, 9), "7203", 2500.0, 2530.0, 2480.0, 2550.0, 110000.0, 1e8),
            (date(2024, 1, 10), "7203", 2550.0, 2560.0, 2540.0, 2600.0, 120000.0, 1e8),
            (date(2024, 1, 11), "7203", 2600.0, 2620.0, 2590.0, 2580.0, 90000.0, 1e8),
            (date(2024, 1, 12), "7203", 2580.0, 2590.0, 2560.0, 2620.0, 95000.0, 1e8),
        ]
        cols = ["trade_date", "code", "open", "high", "low", "close", "volume", "turnover_value"]
        return spark.createDataFrame(data, cols)

    def test_ma5_calculation(self, spark):
        """5日移動平均が正しく計算される"""
        from pyspark.sql import Window
        df = self._make_silver_df(spark)
        w = Window.partitionBy("code").orderBy("trade_date").rowsBetween(-4, 0)
        result = df.withColumn("ma5", F.avg("close").over(w))
        last = result.orderBy("trade_date").tail(1)[0]
        closes = [2500.0, 2550.0, 2600.0, 2580.0, 2620.0]
        expected_ma5 = round(sum(closes) / 5, 6)
        assert abs(last["ma5"] - expected_ma5) < 0.01

    def test_return_1d(self, spark):
        """1日リターンが正しく計算される"""
        from pyspark.sql import Window
        df = self._make_silver_df(spark)
        w = Window.partitionBy("code").orderBy("trade_date")
        result = df.withColumn("return_1d", (F.col("close") / F.lag("close", 1).over(w) - 1) * 100)
        row = result.filter(F.col("trade_date") == date(2024, 1, 12)).first()
        expected = (2620.0 / 2580.0 - 1) * 100
        assert abs(row["return_1d"] - expected) < 0.001


# ── DataQuality ────────────────────────────────────────────────────────────

class TestDataQuality:
    def test_null_check_passes(self, spark):
        data = [
            (date(2024, 1, 5), "7203", 2500.0, 100000.0),
        ]
        df = spark.createDataFrame(data, ["trade_date", "code", "close", "volume"])
        null_count = df.filter(F.col("close").isNull()).count()
        assert null_count == 0

    def test_null_check_fails(self, spark):
        data = [
            (date(2024, 1, 5), "7203", None, 100000.0),
        ]
        df = spark.createDataFrame(data, ["trade_date", "code", "close", "volume"])
        null_count = df.filter(F.col("close").isNull()).count()
        assert null_count == 1

    def test_duplicate_detection(self, spark):
        data = [
            (date(2024, 1, 5), "7203", 2500.0),
            (date(2024, 1, 5), "7203", 2510.0),  # 重複
        ]
        df = spark.createDataFrame(data, ["trade_date", "code", "close"])
        total = df.count()
        unique = df.dropDuplicates(["trade_date", "code"]).count()
        assert total - unique == 1
