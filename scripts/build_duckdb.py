"""
06 · 构建 DuckDB 查询层

把 Parquet 目录挂成 DuckDB 里的视图，产出单一的 warehouse.duckdb 文件。

为什么用视图而不是物化：
  Parquet 本来就是列式压缩的，DuckDB 直接查已经很块；视图零拷贝、无需重建，
  上游一旦重跑数据，查询层自动跟着更新。物化反而多一份存储和一次同步步骤。

用法：
    python scripts/build_duckdb.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

# (schema, 表名, parquet 路径, 是否需要 hive 分区推断)
LAYERS = [
    ("ods", "gh_events", C.ODS_DIR / "**" / "*.parquet", True),
    ("dwd", "gh_events_detail", C.DWD_DIR / "gh_events_detail" / "**" / "*.parquet", True),
    ("dwd", "dim_repo", C.DWD_DIR / "dim_repo" / "*.parquet", False),
    ("dwd", "dim_actor", C.DWD_DIR / "dim_actor" / "*.parquet", False),
    ("dwd", "dim_date", C.DWD_DIR / "dim_date" / "*.parquet", False),
    ("ads", "daily_event_type", C.ADS_DIR / "daily_event_type" / "*.parquet", False),
    ("ads", "daily_repo_rank", C.ADS_DIR / "daily_repo_rank" / "*.parquet", False),
    ("ads", "daily_actor_active", C.ADS_DIR / "daily_actor_active" / "*.parquet", False),
    ("ads", "daily_org_rank", C.ADS_DIR / "daily_org_rank" / "*.parquet", False),
    ("ads", "hourly_activity", C.ADS_DIR / "hourly_activity" / "*.parquet", False),
    ("ads", "daily_pr_funnel", C.ADS_DIR / "daily_pr_funnel" / "*.parquet", False),
]


def main() -> int:
    if C.WAREHOUSE_DB.exists():
        C.WAREHOUSE_DB.unlink()

    con = duckdb.connect(str(C.WAREHOUSE_DB))
    con.execute("PRAGMA threads=4")

    print("=" * 66)
    print(f"构建 DuckDB 查询层 → {C.WAREHOUSE_DB.name}")
    print("=" * 66)

    missing = []
    for schema, name, path, hive in LAYERS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        hive_opt = ", hive_partitioning=true" if hive else ""
        # 视图创建是惰性的，所以「建视图 + count 一次」才是真正的可读性验证。
        # 不去预先判断文件是否存在 —— `**/*.parquet` 这种通配符用 Path.glob 判断
        # 很容易出错（parent 是 `data/ods/**` 这样的字面量目录），直接试更可靠。
        try:
            t0 = time.time()
            con.execute(f"CREATE OR REPLACE VIEW {schema}.{name} AS "
                        f"SELECT * FROM read_parquet('{path.as_posix()}'{hive_opt})")
            n = con.execute(f"SELECT count(*) FROM {schema}.{name}").fetchone()[0]
            print(f"  {schema}.{name:<20} {n:>10,} 行   {time.time() - t0:5.2f}s")
        except Exception as exc:
            con.execute(f"DROP VIEW IF EXISTS {schema}.{name}")
            missing.append(f"{schema}.{name}")
            print(f"  {schema}.{name:<20} 跳过（{type(exc).__name__}）")

    print("-" * 66)
    tables = con.execute("""
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_schema IN ('ods','dwd','ads') ORDER BY 1, 2
    """).fetchall()
    print(f"共 {len(tables)} 个视图已挂载")
    if missing:
        print(f"⚠️  未挂载：{', '.join(missing)}")

    size = C.WAREHOUSE_DB.stat().st_size / 1024
    print(f"warehouse.duckdb  {size:.1f} KB（只有元数据，数据仍指向 Parquet）")
    print("=" * 66)

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
