"""Data Quality チェック"""
from dataclasses import dataclass, field
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.common.utils import get_logger, load_config

logger = get_logger(__name__)


@dataclass
class DQResult:
    check_name: str
    passed: bool
    detail: str
    row_count: int = 0
    failed_count: int = 0


@dataclass
class DQReport:
    table: str
    results: list[DQResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        lines = [f"=== DQ Report [{self.table}]: {status} ==="]
        for r in self.results:
            icon = "✅" if r.passed else "❌"
            lines.append(f"  {icon} {r.check_name}: {r.detail}")
        return "\n".join(lines)


class DataQualityChecker:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        cfg = load_config()
        self.catalog = cfg["databricks"]["catalog"]
        self.silver_schema = cfg["databricks"]["schema_silver"]

    def _full(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    def check_silver_daily_quotes(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        fail_on_error: bool = False,
    ) -> DQReport:
        table = self._full(self.silver_schema, "daily_quotes")
        df = self.spark.table(table)

        if date_from:
            df = df.filter(F.col("trade_date") >= date_from)
        if date_to:
            df = df.filter(F.col("trade_date") <= date_to)

        report = DQReport(table=table)
        total = df.count()

        # 1. Not Null チェック
        for col in ["trade_date", "code", "close", "volume"]:
            nulls = df.filter(F.col(col).isNull()).count()
            report.results.append(DQResult(
                check_name=f"not_null:{col}",
                passed=(nulls == 0),
                detail=f"{nulls} nulls / {total} rows",
                row_count=total,
                failed_count=nulls,
            ))

        # 2. close > 0
        neg_close = df.filter(F.col("close") <= 0).count()
        report.results.append(DQResult(
            check_name="close_positive",
            passed=(neg_close == 0),
            detail=f"{neg_close} records with close<=0",
            row_count=total,
            failed_count=neg_close,
        ))

        # 3. high >= low
        bad_hl = df.filter(F.col("high") < F.col("low")).count()
        report.results.append(DQResult(
            check_name="high_gte_low",
            passed=(bad_hl == 0),
            detail=f"{bad_hl} records where high < low",
            row_count=total,
            failed_count=bad_hl,
        ))

        # 4. 重複チェック
        dupes = total - df.dropDuplicates(["trade_date", "code"]).count()
        report.results.append(DQResult(
            check_name="no_duplicates",
            passed=(dupes == 0),
            detail=f"{dupes} duplicate (trade_date, code) combinations",
            row_count=total,
            failed_count=dupes,
        ))

        # 5. volume >= 0
        neg_vol = df.filter(F.col("volume") < 0).count()
        report.results.append(DQResult(
            check_name="volume_non_negative",
            passed=(neg_vol == 0),
            detail=f"{neg_vol} records with volume<0",
            row_count=total,
            failed_count=neg_vol,
        ))

        logger.info(report.summary())

        if fail_on_error and not report.passed:
            raise ValueError(f"DQ checks failed for {table}")

        return report

    def save_dq_report(self, report: DQReport, spark: SparkSession = None):
        """DQ結果をDeltaテーブルに記録"""
        sp = spark or self.spark
        rows = []
        for r in report.results:
            rows.append({
                "checked_at": __import__("datetime").datetime.now().isoformat(),
                "table_name": report.table,
                "check_name": r.check_name,
                "passed": r.passed,
                "detail": r.detail,
                "row_count": r.row_count,
                "failed_count": r.failed_count,
            })
        df = sp.createDataFrame(rows)
        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{self.catalog}.{self.silver_schema}.dq_log")
        )
