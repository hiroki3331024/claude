# Azure 統合（Professional 保有者の最重点学習エリア）

---

## ADLS Gen2 アクセス方法（必須）

### 方法1: サービスプリンシパル + OAuth（推奨）

```python
spark.conf.set("fs.azure.account.auth.type.<storage>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage>.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage>.dfs.core.windows.net", "<client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage>.dfs.core.windows.net",
               dbutils.secrets.get("kv-scope", "sp-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage>.dfs.core.windows.net",
               "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
```

### 方法2: マネージド ID（クラスタに割り当てた場合）

```python
spark.conf.set("fs.azure.account.auth.type.<storage>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage>.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
```

### 方法3: ストレージアカウントキー（非推奨）

```python
spark.conf.set(
    "fs.azure.account.key.<storage>.dfs.core.windows.net",
    dbutils.secrets.get("kv-scope", "storage-key")
)
```

### URI 形式

```
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```

---

## Unity Catalog との統合

```
Storage Credential（Managed Identity or SP）
    ↓
External Location（abfss://... パスを登録）
    ↓
External Table / Managed Table
```

```sql
-- Storage Credential 作成
CREATE STORAGE CREDENTIAL my_cred
WITH AZURE MANAGED IDENTITY
(DIRECTORY = '/subscriptions/.../resourceGroups/.../providers/Microsoft.Databricks/accessConnectors/my-connector');

-- External Location 作成
CREATE EXTERNAL LOCATION my_location
URL 'abfss://data@mystorageaccount.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL my_cred);

-- 確認
VALIDATE STORAGE CREDENTIAL my_cred;
SHOW EXTERNAL LOCATIONS;
```

---

## Azure Data Factory (ADF) 統合

### Linked Service の作成

- 認証方式: Managed Identity または サービスプリンシパル
- ワークスペース URL とリソース ID を入力

### ADF から Databricks を呼ぶパターン

| ADF アクティビティ | 内容 |
|------------------|------|
| Notebook アクティビティ | ノートブック実行・パラメータ渡し可能 |
| JAR アクティビティ | Java/Scala JAR 実行 |
| Python アクティビティ | .py ファイル実行 |

### よくある使い方

```
ADF Pipeline
  ├── Copy Data（Blob → ADLS）
  └── Databricks Notebook（Bronze → Silver 変換）
```

---

## Azure Key Vault 統合

### シークレットスコープの作成

```bash
# Databricks CLI
databricks secrets create-scope \
  --scope azure-kv \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/.../vaults/my-keyvault \
  --dns-name https://my-keyvault.vault.azure.net/
```

### ノートブックからの利用

```python
# 値は表示されない（マスクされる）
password = dbutils.secrets.get(scope="azure-kv", key="db-password")
connection_string = dbutils.secrets.get(scope="azure-kv", key="storage-connection")
```

---

## Entra ID（旧 Azure AD）

### 認証方式の選択

| 方式 | 用途 | 推奨度 |
|------|------|--------|
| マネージド ID | Azure リソース間（ADF→Databricks等） | 最推奨 |
| サービスプリンシパル | CI/CD、外部サービス | 推奨 |
| ワークロード ID フェデレーション | GitHub Actions 等 | 状況による |
| 条件付きアクセス | ユーザーアクセス制御 | セキュリティ強化 |

### サービスプリンシパル登録

```bash
# Azure CLI
az ad sp create-for-rbac --name "databricks-sp" --role Contributor \
  --scopes /subscriptions/<subscription-id>

# Databricks にサービスプリンシパルを登録
databricks service-principals create --application-id <app-id>
```

---

## Azure Monitor 統合

### 診断ログのカテゴリ

| カテゴリ | 内容 |
|---------|------|
| clusters | クラスタ起動・停止イベント |
| jobs | ジョブ実行ログ |
| notebook | ノートブック実行ログ |
| accounts | ユーザーログイン |
| sqlPermissions | Unity Catalog アクセスログ |

### Log Analytics での分析（KQL）

```kql
DatabricksJobs
| where TimeGenerated > ago(24h)
| where actionName == "runFailed"
| project TimeGenerated, userIdentity, requestId
| order by TimeGenerated desc
```

---

## ネットワーク（VNet / Private Link）

### VNet インジェクション構成

```
カスタム VNet
├── パブリックサブネット（worker nodes 用）
│   └── NSG: Databricks が自動管理
└── プライベートサブネット（driver node 用）
    └── NSG: Databricks が自動管理
```

### No Public IP (NPIP) + Private Link

- クラスタノードがパブリック IP を持たない構成
- ワークスペースへのアクセスはプライベートエンドポイント経由
- 最もセキュアな構成（金融・医療向け）

---

## 参考リンク

- [ADLS Gen2 アクセス設定](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage)
- [Key Vault シークレットスコープ](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes)
- [Entra ID / サービスプリンシパル](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/users-groups/service-principals)
- [Azure Monitor 診断ログ](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/account-settings/azure-diagnostic-logs)
- [VNet インジェクション](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/vnet-inject)
