"""
全局配置 —— 所有脚本统一从这里取路径、时间范围与并发参数。

改参数有两种方式：
  1) 直接改本文件；
  2) 用环境变量临时覆盖：GH_DATES / GH_WORKERS / GH_RETRY / GH_TIMEOUT
"""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent

DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"      # 原始 gz，按 dt=YYYY-MM-DD 分区
ODS_DIR = DATA_DIR / "ods"      # ODS：顶层字段展开 + payload 原样保留
DWD_DIR = DATA_DIR / "dwd"      # DWD：事实表 + 维度表
ADS_DIR = DATA_DIR / "ads"      # ADS：面向分析的应用宽表
REPORT_DIR = ROOT / "reports"   # 数据质量报告
WAREHOUSE_DB = ROOT / "warehouse.duckdb"

ARCHIVE_BASE = "https://data.gharchive.org"
USER_AGENT = "Mozilla/5.0 (compatible; gh-eco-platform/0.1)"

HOURS = list(range(24))

MAX_WORKERS = int(os.getenv("GH_WORKERS", "6"))
RETRY = int(os.getenv("GH_RETRY", "3"))
TIMEOUT = int(os.getenv("GH_TIMEOUT", "60"))


DEMO_DB = ROOT / "demo" / "demo.duckdb"


def resolve_warehouse() -> tuple[Path, str]:
    """选择要用的 DuckDB 文件，返回 (路径, 类型)。

    完整数据约 15 GB，不可能进 Git 仓库，所以额外产出一份 2 MB 的演示数据集
    （`scripts/build_demo_data.py`），让任何人 clone 下来就能跑起看板和问数。
    优先用完整仓库，找不到才回落。

    dashboard.py 与 text2sql/agent.py 都走这个函数，避免两处逻辑不一致。
    """
    if WAREHOUSE_DB.exists():
        return WAREHOUSE_DB, "full"
    if DEMO_DB.exists():
        return DEMO_DB, "demo"
    return WAREHOUSE_DB, "missing"


def ensure_dirs() -> None:
    for d in (RAW_DIR, ODS_DIR, DWD_DIR, ADS_DIR, REPORT_DIR):
        d.mkdir(parents=True, exist_ok=True)


def default_dates(days: int = 7) -> list[str]:
    """最近 days 个完整自然日（不含今天），返回 ISO 日期字符串列表。"""
    end = dt.date.today() - dt.timedelta(days=1)
    start = end - dt.timedelta(days=days - 1)
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out


def dates_from_env(default_days: int = 7) -> list[str]:
    """优先读环境变量 GH_DATES=2026-09-01,2026-09-02；否则取最近 default_days 天。"""
    raw = os.getenv("GH_DATES", "").strip()
    if raw:
        return [d.strip() for d in raw.split(",") if d.strip()]
    return default_dates(default_days)


ensure_dirs()
