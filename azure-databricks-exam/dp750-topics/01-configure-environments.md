# ドメイン1: 環境構成（15〜20%）

---

## Azure Databricks ワークスペース

### プラン比較

| 機能 | Standard | Premium |
|------|----------|---------|
| Unity Catalog | 非対応 | 対応 |
| Row/Column Level Security | なし | あり |
| IP アクセスリスト | なし | あり |
| SLA | 99.95% | 99.95% |

### ワークスペース作成（ARM / Terraform）

```hcl
# Terraform での作成例
resource "azurerm_databricks_workspace" "example" {
  name                = "example-workspace"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location
  sku                 = "premium"

  custom_parameters {
    virtual_network_id = azurerm_virtual_network.example.id
    public_subnet_name = "public-subnet"
    private_subnet_name = "private-subnet"
  }
}
```

---

## クラスタ管理

### クラスタタイプ

| タイプ | 用途 |
|--------|------|
| All-purpose | 対話型・開発用 |
| Job クラスタ | ジョブ実行専用（安価） |
| SQL Warehouse (Serverless) | SQL 分析・BI ツール接続 |
| SQL Warehouse (Pro) | 高度なクエリ最適化 |
| SQL Warehouse (Classic) | 旧来型 SQL |

### クラスタポリシー

```json
{
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
  },
  "autoscale.max_workers": {
    "type": "range",
    "maxValue": 10
  }
}
```

---

## ネットワーク設定

### VNet インジェクション

- Databricks をカスタム VNet 内にデプロイ
- パブリックサブネット・プライベートサブネットが必要
- NSG ルールを Databricks が管理

### Private Link

- ワークスペースへのアクセスをプライベートエンドポイント経由に制限
- No Public IP (NPIP) と組み合わせてセキュアな構成

### IP アクセスリスト（Premium のみ）

```python
# Databricks REST API で設定
POST /api/2.0/ip-access-lists
{
  "label": "corporate-vpn",
  "list_type": "ALLOW",
  "ip_addresses": ["203.0.113.0/24"]
}
```

---

## 認証・ID 管理

| 方式 | 説明 |
|------|------|
| Entra ID (Azure AD) | SSO・条件付きアクセス |
| サービスプリンシパル | CI/CD・自動化用 |
| マネージド ID | Azure リソース間の認証（推奨） |
| PAT | 個人アクセストークン（非推奨・移行予定） |

---

## 参考リンク

- [VNet インジェクション](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/vnet-inject)
- [Private Link](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/private-link)
- [クラスタポリシー](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/clusters/policy-definition)
