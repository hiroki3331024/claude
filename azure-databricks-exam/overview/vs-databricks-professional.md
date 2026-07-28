# DP-750 vs Databricks Certified Data Engineer Professional

## 資格比較表

| 項目 | DP-750 | Databricks Professional |
|------|--------|------------------------|
| 発行機関 | Microsoft | Databricks |
| レベル | Associate | Professional |
| クラウド範囲 | Azure 限定 | マルチクラウド |
| 試験時間 | 約100分 | 120分 |
| 問題数 | 40〜60問 | 59問 |
| 難易度 | 中 | 高 |
| 有効期限 | 2年 | 2年 |

---

## Databricks Professional にしかない知識

- Spark 内部処理（DAG、Shuffle、メモリ管理）
- Spark UI 読み方・チューニング
- Delta Live Tables の高度な機能
- CLI / REST API 操作
- マルチクラウド（AWS S3、GCS）
- Databricks コスト最適化

---

## DP-750 にしかない知識（← ここを重点学習）

- ADLS Gen2 認証・アクセス（3通りの方法）
- Azure Data Factory (ADF) との統合
- Azure Key Vault シークレットスコープ
- Entra ID（旧 Azure AD）サービスプリンシパル管理
- Azure Monitor への診断ログ送信
- VNet インジェクション / Private Link
- ARM テンプレート / Terraform での Databricks 構築
- Unity Catalog の Azure 固有設定（Storage Credential、External Location）

---

## 共通知識（Professional があれば余裕）

- Delta Lake（ACID、タイムトラベル、スキーマ進化）
- Unity Catalog の基本（3階層、権限管理）
- Medallion Architecture
- Auto Loader
- Databricks Jobs / ワークフロー
- SQL / Python / Spark DataFrame API

---

## Professional 保有者の学習戦略

1. **Databricks 操作部分はほぼスキップ可能**（既習）
2. **Azure 固有の統合部分に集中**（学習の70〜80%）
3. Associate レベルなので Spark 内部の深掘りは不要
4. Azure ポータルでの UI 操作と CLI コマンドを覚える
