# ドメイン2: データ取込と変換（30〜35%）

---

## Auto Loader（増分取込の推奨方式）

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "abfss://checkpoints@storage.dfs.core.windows.net/schema")
    .load("abfss://raw@storage.dfs.core.windows.net/events/")
)

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "abfss://checkpoints@storage.dfs.core.windows.net/events") \
    .outputMode("append") \
    .table("bronze.events")
```

### Auto Loader vs COPY INTO

| 観点 | Auto Loader | COPY INTO |
|------|------------|----------|
| 処理方式 | ストリーミング | バッチ |
| スケーラビリティ | 数百万ファイル対応 | 数千ファイル向き |
| スキーマ推論 | 自動 | 手動指定 |
| 推奨用途 | 継続的インジェスト | 定期バッチ取込 |

---

## COPY INTO

```sql
COPY INTO bronze.orders
FROM 'abfss://raw@storage.dfs.core.windows.net/orders/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true')
```

---

## ADF との統合

### パターン

1. **ADF → Databricks Notebook**: Notebook アクティビティで直接呼び出し
2. **ADF → Databricks JAR**: JAR アクティビティ
3. **ADF トリガー → Databricks Jobs**: Event-based trigger → REST API

```json
// ADF Linked Service（サービスプリンシパル認証）
{
  "type": "AzureDatabricks",
  "typeProperties": {
    "domain": "https://adb-xxx.azuredatabricks.net",
    "authentication": "MSI",
    "workspaceResourceId": "/subscriptions/.../resourceGroups/.../providers/..."
  }
}
```

---

## Delta Lake 操作

### MERGE（Upsert）

```sql
MERGE INTO silver.customers AS target
USING bronze.customers_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET target.email = source.email, target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, created_at) VALUES (source.customer_id, source.email, source.created_at);
```

### Time Travel

```sql
-- バージョン指定
SELECT * FROM silver.orders VERSION AS OF 5;

-- タイムスタンプ指定
SELECT * FROM silver.orders TIMESTAMP AS OF '2024-01-15 00:00:00';

-- 変更履歴確認
DESCRIBE HISTORY silver.orders;
```

---

## Structured Streaming

```python
from pyspark.sql.functions import window, col

# ウォーターマーク付きウィンドウ集計
df_agg = (
    df.withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "5 minutes"), col("device_id"))
    .count()
)
```

---

## Lakeflow Pipelines（旧 Delta Live Tables）

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Raw events from ADLS")
def bronze_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("abfss://raw@storage.dfs.core.windows.net/events/")
    )

@dlt.table()
@dlt.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
def silver_events():
    return (
        dlt.read_stream("bronze_events")
        .filter(col("event_type").isNotNull())
    )
```

### Medallion Architecture

| レイヤー | 内容 | 形式 |
|---------|------|------|
| Bronze | 生データ（無変換） | Delta |
| Silver | クレンジング・結合済み | Delta |
| Gold | ビジネス集計・KPI | Delta |

---

## 参考リンク

- [Auto Loader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/)
- [COPY INTO](https://learn.microsoft.com/en-us/azure/databricks/ingestion/copy-into/)
- [Lakeflow Pipelines](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/)
