# ドメイン3: データモデリングと最適化（20〜25%）

---

## Unity Catalog（ガバナンスの中核）

### 3階層構造

```
Catalog
 └── Schema (Database)
      └── Table / View / Function / Volume
```

### 主な操作

```sql
-- カタログ作成
CREATE CATALOG IF NOT EXISTS prod;

-- スキーマ作成
CREATE SCHEMA IF NOT EXISTS prod.sales;

-- 権限付与
GRANT SELECT ON TABLE prod.sales.orders TO `analysts@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA prod.sales TO `data-engineers`;

-- External Location 確認
SHOW EXTERNAL LOCATIONS;
```

### Azure 固有の設定

| 設定項目 | 内容 |
|---------|------|
| Storage Credential | Managed Identity or サービスプリンシパルを登録 |
| External Location | Storage Credential + ADLS パスを紐付け |
| メタストア | Azure Databricks アカウントレベルで1リージョン1つ |

---

## Delta Lake 最適化

### OPTIMIZE と Z-ORDER

```sql
-- ファイルのコンパクション
OPTIMIZE silver.orders;

-- Z-ORDER（特定列でのデータスキッピング最適化）
OPTIMIZE silver.orders ZORDER BY (customer_id, order_date);
```

### VACUUM（古いファイルの削除）

```sql
-- デフォルト7日保持（デルタログ保持と一致させる）
VACUUM silver.orders RETAIN 168 HOURS;

-- 確認モード
VACUUM silver.orders RETAIN 168 HOURS DRY RUN;
```

### テーブル統計情報

```sql
ANALYZE TABLE silver.orders COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## パーティショニング戦略

| 方式 | 特徴 | 適用場面 |
|------|------|---------|
| Hive スタイルパーティション | ディレクトリ分割 | 大量データ・日付パーティション |
| Liquid Clustering | 動的クラスタリング（推奨） | 多様なクエリパターン |
| Z-ORDER | ファイル内ソート | 読み取り最適化 |

### Liquid Clustering（推奨）

```sql
CREATE TABLE silver.orders
CLUSTER BY (customer_id, order_date);

-- 定期的なクラスタリング
OPTIMIZE silver.orders;
```

---

## データ品質

```python
# Lakeflow Pipelines での期待値定義
@dlt.expect("valid_date", "order_date IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect_or_fail("required_id", "order_id IS NOT NULL")
```

---

## 参考リンク

- [Unity Catalog ベストプラクティス](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/best-practices)
- [Delta Lake 最適化](https://learn.microsoft.com/en-us/azure/databricks/delta/optimizations/)
- [Liquid Clustering](https://learn.microsoft.com/en-us/azure/databricks/delta/clustering)
