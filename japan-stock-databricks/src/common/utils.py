"""共通ユーティリティ関数"""
import logging
import time
import functools
from datetime import datetime, timedelta
from typing import Any, Callable, Optional
import yaml
import os


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def load_config(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "../../config/config.yml"
        )
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return _resolve_env_vars(raw)


def _resolve_env_vars(obj: Any) -> Any:
    if isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
        key = obj[2:-1]
        return os.environ.get(key, obj)
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(i) for i in obj]
    return obj


def retry(max_attempts: int = 3, wait_seconds: float = 5.0, exceptions: tuple = (Exception,)):
    """リトライデコレータ"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    logger.warning(f"{func.__name__} attempt {attempt} failed: {e}. Retrying in {wait_seconds}s...")
                    time.sleep(wait_seconds)
        return wrapper
    return decorator


def date_range(start: str, end: str, fmt: str = "%Y-%m-%d"):
    """start から end までの日付リストを返す"""
    s = datetime.strptime(start, fmt)
    e = datetime.strptime(end, fmt)
    delta = e - s
    return [(s + timedelta(days=i)).strftime(fmt) for i in range(delta.days + 1)]


def get_date_n_years_ago(n: int = 3) -> str:
    return (datetime.now() - timedelta(days=365 * n)).strftime("%Y-%m-%d")


def get_today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def get_yesterday() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
