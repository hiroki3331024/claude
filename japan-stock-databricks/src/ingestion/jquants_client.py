"""J-Quants API クライアント"""
import json
import os
import time
from datetime import datetime
from typing import Optional
import requests

from src.common.utils import get_logger, retry, load_config

logger = get_logger(__name__)


class JQuantsClient:
    """J-Quants API クライアント

    J-Quants Free プランで利用可能なエンドポイントに対応。
    認証トークンをメモリにキャッシュし、期限切れ時は自動リフレッシュする。
    """

    BASE_URL = "https://api.jquants.com/v1"

    def __init__(self, id_token: str):
        """
        Args:
            id_token: J-Quantsポータルの「アクセストークン」をそのまま渡す
        """
        self._id_token = id_token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._id_token}"}

    # ── API リクエスト共通 ─────────────────────────────────────────────────

    @retry(max_attempts=3, wait_seconds=5)
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.BASE_URL}{path}"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
        if resp.status_code == 401:
            # トークン期限切れ → 強制リフレッシュ
            self._token_expires_at = 0
            self._ensure_token()
            resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ── エンドポイント ─────────────────────────────────────────────────────

    def get_listed_info(self) -> list[dict]:
        """上場銘柄一覧を取得"""
        data = self._get("/listed/info")
        return data.get("info", [])

    def get_prices_daily_quotes(
        self,
        code: Optional[str] = None,
        date: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict]:
        """日次株価データを取得

        Args:
            code: 銘柄コード（省略時は全銘柄）
            date: 特定日付 (YYYYMMDD or YYYY-MM-DD)
            date_from: 開始日
            date_to:   終了日
        """
        params = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date.replace("-", "")
        if date_from:
            params["from"] = date_from.replace("-", "")
        if date_to:
            params["to"] = date_to.replace("-", "")

        data = self._get("/prices/daily_quotes", params=params)
        return data.get("daily_quotes", [])

    def get_statements(
        self,
        code: Optional[str] = None,
        date: Optional[str] = None,
    ) -> list[dict]:
        """財務情報を取得（Free プランは直近4件）"""
        params = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date.replace("-", "")
        data = self._get("/fins/statements", params=params)
        return data.get("statements", [])

    # ── Raw 保存ユーティリティ ─────────────────────────────────────────────

    def save_raw_json(self, data: list[dict], category: str, partition_key: str, base_path: str):
        """取得データをRaw JSONとして保存"""
        path = os.path.join(base_path, category, f"{partition_key}.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved raw JSON: {path} ({len(data)} records)")
        return path
