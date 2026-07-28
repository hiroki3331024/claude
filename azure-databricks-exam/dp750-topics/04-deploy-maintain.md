# ドメイン4: デプロイ・運用（20〜25%）

---

## CI/CD と Git 連携

### Databricks Git Folders（Repos）
- GitHub / Azure DevOps / GitLab 連携
- ブランチ管理・プルリクエストのサポート
- ノートブックのバージョン管理

### Databricks Asset Bundles (DAB)
Databricks 推奨の IaC ツール。

```yaml
# databricks.yml
bundle:
  name: my-pipeline

targets:
  dev:
    workspace:
      host: https://adb-xxx.azuredatabricks.net
  prod:
    workspace:
      host: https://adb-yyy.azuredatabricks.net

resources:
  jobs:
    etl_job:
      name: ETL Pipeline
      tasks:
        - task_key: transform
          notebook_task:
            notebook_path: ./notebooks/transform
```

```bash
# デプロイ
databricks bundle deploy --target prod

# 実行
databricks bundle run etl_job --target prod
```

---

## Databricks Jobs（オーケストレーション）

### ジョブタスクタイプ

| タイプ | 内容 |
|--------|------|
| Notebook | ノートブック実行 |
| Python Script | .py ファイル実行 |
| Delta Live Tables Pipeline | Lakeflow Pipeline 実行 |
| SQL | SQL クエリ実行 |
| dbt | dbt モデル実行 |
| Run Job | 別ジョブの呼び出し |

### 依存関係とリトライ

```python
# REST API でのジョブ作成（概念）
{
  "name": "ETL Job",
  "tasks": [
    {"task_key": "ingest", ...},
    {"task_key": "transform", "depends_on": [{"task_key": "ingest"}], ...}
  ],
  "max_concurrent_runs": 1,
  "retry_on_timeout": true
}
```

---

## 監視・アラート

### Databricks 組み込み監視

- ジョブ実行履歴・ログ
- クラスタイベントログ
- Query History（SQL Warehouse）

### Azure Monitor 統合

- 診断ログ → Log Analytics ワークスペース
- アラートルール → メール / Teams / PagerDuty

### ノートブックからのカスタムメトリクス送信

```python
# カスタムメトリクスのログ（MLflow 活用）
import mlflow

with mlflow.start_run():
    mlflow.log_metric("records_processed", df.count())
    mlflow.log_metric("error_count", errors.count())
```

---

## セキュリティ運用

### シークレット管理

```python
# Key Vault スコープからシークレット取得
storage_key = dbutils.secrets.get(scope="azure-kv", key="storage-account-key")

# 接続文字列の組み立て
spark.conf.set(
    "fs.azure.account.key.mystorageaccount.dfs.core.windows.net",
    storage_key
)
```

### 監査ログの活用
- Unity Catalog のデータアクセスログ
- Entra ID サインインログとの相関分析

---

## トラブルシューティング

| 症状 | 確認箇所 |
|------|---------|
| ジョブ失敗 | ドライバーログ / stderr |
| クラスタ起動失敗 | クラスタイベントログ |
| パフォーマンス低下 | Spark UI (Stages / Tasks) |
| 認証エラー | Entra ID / サービスプリンシパルの権限 |
| ストレージアクセスエラー | ADLS の RBAC / Storage Credential 設定 |

---

## 参考リンク

- [Databricks Asset Bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- [Jobs オーケストレーション](https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/)
- [Azure Monitor 統合](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/account-settings/azure-diagnostic-logs)
