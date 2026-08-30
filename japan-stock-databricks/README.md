# Japan Stock Databricks Pipeline

J-Quants API から日本株データを取得し、Databricks Free Edition 上で  
Bronze → Silver → Gold の Medallion Architecture で管理・分析するデータ基盤です。

---

## アーキテクチャ

```
J-Quants API
    │
    ▼
[Ingestion] jquants_client.py
    │  (Raw JSON を /dbfs/raw/ に保存)
    ▼
[Bronze] Delta Table ─ 生データをそのまま格納 (MERGE upsert)
    │
    ▼
[Silver] Delta Table ─ クレンジング・型変換・正規化 (MERGE upsert)
    │           │
    │         [DQ Check] Data Quality ログ記録
    ▼
[Gold]  Delta Table ─ テクニカル指標・集計・銘柄マスタ結合
    │
    ├── Databricks SQL Dashboard  (騰落率・セクター別・チャート)
    └── Databricks Genie          (自然言語でデータ探索)
```

## ディレクトリ構成

```
japan-stock-databricks/
├── src/
│   ├── ingestion/
│   │   └── jquants_client.py      # J-Quants API クライアント
│   ├── bronze/
│   │   └── load_bronze.py         # Raw JSON → Bronze Delta
│   ├── silver/
│   │   └── build_silver.py        # Bronze → Silver 変換・MERGE
│   ├── gold/
│   │   └── build_gold.py          # Silver → Gold 指標計算
│   ├── quality/
│   │   └── data_quality.py        # DQ チェック・ログ
│   └── common/
│       └── utils.py               # 共通関数
│
├── notebooks/
│   ├── 01_initial_load.py         # 初回3年分ロード
│   ├── 02_incremental_load.py     # 日次増分ロード
│   └── 03_run_pipeline.py         # 自動判定・実行制御
│
├── sql/
│   ├── create_schemas.sql         # スキーマ・テーブル作成
│   ├── validation_queries.sql     # 検証・モニタリングクエリ
│   └── dashboard_queries.sql      # Dashboard / Genie 用クエリ
│
├── config/
│   ├── config.yml                 # パラメーター設定
│   └── workflow_definition.json   # Databricks Workflow 定義
│
├── tests/
│   └── test_transformations.py    # pytest ユニットテスト
│
├── docs/
│   └── setup_and_execution_guide.html  # 実行手順書 (HTML)
│
├── README.md
├── .gitignore
└── requirements.txt
```

## 使用技術

| カテゴリ | 技術 |
|---|---|
| データソース | J-Quants API (無料プラン) |
| 処理エンジン | Apache Spark (Databricks) |
| ストレージ形式 | Delta Lake |
| メタデータ管理 | Unity Catalog |
| ワークフロー | Databricks Workflows |
| 可視化 | Databricks SQL Dashboard |
| AI 探索 | Databricks Genie |
| テスト | pytest + PySpark |

## セットアップ概要

1. J-Quants アカウント取得 (無料登録)
2. Databricks Free Edition アカウント作成
3. GitHub リポジトリ → Databricks Repos で連携
4. Databricks Secrets に J-Quants 認証情報を登録
5. `01_initial_load.py` で初回3年分データを取得
6. Workflow を設定して毎朝7時に自動実行

詳細な手順は **[HTML 手順書](docs/setup_and_execution_guide.html)** を参照してください。

## 実行方法

### 初回ロード
```
Databricks Workspace > Repos > <このリポジトリ>
> notebooks/01_initial_load.py を開いて [Run All]
```

### 日次増分ロード (手動)
```
notebooks/02_incremental_load.py を開いて [Run All]
```

### Workflow による自動化
```
Workflows > Create Job > workflow_definition.json の設定を参考に構成
毎朝 07:00 JST に自動実行
```

### ローカルテスト
```bash
pip install -r requirements.txt
pytest tests/ -v
```

## テーブル一覧

| レイヤー | テーブル | 説明 |
|---|---|---|
| Bronze | `main.bronze_stocks.daily_quotes` | 日次株価 (Raw) |
| Bronze | `main.bronze_stocks.listed_info` | 上場銘柄情報 (Raw) |
| Silver | `main.silver_stocks.daily_quotes` | クレンジング済み株価 |
| Silver | `main.silver_stocks.listed_info` | クレンジング済み銘柄情報 |
| Silver | `main.silver_stocks.dq_log` | DQ チェック結果ログ |
| Gold | `main.gold_stocks.stock_metrics` | テクニカル指標付き株価 |
| Gold | `main.gold_stocks.stock_with_master` | 銘柄マスタ結合済み |
| Gold | `main.gold_stocks.sector_summary` | セクター別日次集計 |

## 計算される指標

- **移動平均**: MA5, MA25, MA75
- **リターン**: 1日・5日・21日
- **ボラティリティ**: 20日標準偏差
- **出来高移動平均**: 5日
- **52週高値・安値**: 乖離率
