# 学習計画（Databricks Professional 保有者向け）

## 想定学習期間：4〜6週間

Databricks Professional の知識を前提に、**Azure 固有領域に絞って**学習する。

---

## Week 1: Azure 基礎補完

### 目標
Azure Databricks 固有のサービス統合を理解する

### タスク
- [ ] Azure Databricks ワークスペースを無料アカウントで作成
- [ ] ADLS Gen2 のセットアップ・マウント（3通りの認証方法を試す）
- [ ] Azure Key Vault スコープの作成と `dbutils.secrets` での利用
- [ ] Microsoft Learn: [Azure Databricks の概要](https://learn.microsoft.com/en-us/training/paths/data-engineer-azure-databricks/)

---

## Week 2: Unity Catalog の Azure 固有設定

### 目標
Unity Catalog の Azure 環境での設定方法を習得

### タスク
- [ ] Storage Credential の作成（Managed Identity 使用）
- [ ] External Location の設定・確認
- [ ] Unity Catalog でのカタログ・スキーマ・テーブル作成
- [ ] GRANT / REVOKE の権限管理を実践
- [ ] Microsoft Learn: [Unity Catalog モジュール](https://learn.microsoft.com/en-us/training/modules/get-started-azure-databricks-unity-catalog/)

---

## Week 3: データ取込・パイプライン（Azure 統合）

### 目標
Azure 環境での取込パターンとパイプライン構築

### タスク
- [ ] Auto Loader で ADLS Gen2 からの取込を実装
- [ ] Lakeflow Pipelines で Medallion Architecture を構築
- [ ] ADF から Databricks Notebook を呼び出す連携を試す
- [ ] Databricks Jobs でマルチタスクパイプラインを作成

---

## Week 4: デプロイ・運用・セキュリティ

### 目標
本番運用に必要な CI/CD・監視・セキュリティを理解

### タスク
- [ ] Databricks Asset Bundles (DAB) で dev/prod デプロイを試す
- [ ] Git Folders で Azure DevOps / GitHub 連携を設定
- [ ] Azure Monitor 診断ログの送信設定
- [ ] Entra ID サービスプリンシパル認証の設定

---

## Week 5: 模擬試験・弱点補強

### タスク
- [ ] DP-750 公式模擬試験（Microsoft Learn AI Skills Navigator）
- [ ] Udemy 模擬試験（DP-750 Practice Tests）を2周
- [ ] 間違えた問題の該当ドキュメントを読む
- [ ] 弱点ドメインのハンズオンを追加実施

---

## Week 6: 最終確認・受験

### タスク
- [ ] スタディガイドで未カバーの項目をチェック
- [ ] 模擬試験で700点以上が安定して取れることを確認
- [ ] 受験申込・受験

---

## 参考教材リスト

| 教材 | 形式 | 優先度 |
|------|------|--------|
| [DP-750 公式スタディガイド](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/dp-750) | 無料 | 必須 |
| [Microsoft Learn ラーニングパス](https://learn.microsoft.com/en-us/training/paths/data-engineer-azure-databricks/) | 無料 | 必須 |
| [AI Skills Navigator 模擬試験](https://learn.microsoft.com/en-us/credentials/certifications/implementing-data-engineering-solutions-using-azure-databricks/) | 無料 | 必須 |
| Udemy: DP-750 Practice Tests | 有料 | 推奨 |
| [Azure Databricks 公式ドキュメント](https://learn.microsoft.com/en-us/azure/databricks/) | 無料 | 参照 |
