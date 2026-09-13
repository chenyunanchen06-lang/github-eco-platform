"""
03 · ODS 层验收 / 数据概览

用 DuckDB 直接读 Parquet 目录，一次性输出第 1 周的验收指标。
这一步跑通了，才说明"垂直切片"真的打通了。

用法：
    python scripts/check_ods.py
    python scripts/check_ods.py --top 15
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C


def q(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def main() -> int:
    ap = argparse.ArgumentParser(description="ODS 层验收")
    ap.add_argument("--top", type=int, default=10, help="排行榜取前 N 条")
    args = ap.parse_args()

    glob = (C.ODS_DIR / "dt=*" / "hour=*" / "*.parquet").as_posix()
    con = duckdb.connect()

    total = q(con, f"SELECT count(*) FROM read_parquet('{glob}')")[0][0]
    if total == 0:
        print(f"没读到数据。请先运行 01 下载与 02 转换脚本。\n路径：{glob}")
        return 1

    print("=" * 62)
    print(" ODS 层验收报告")
    print("=" * 62)

    rows = q(con, f"""
        SELECT
            count(*)                                              AS 总行数,
            count(DISTINCT event_id)                              AS 唯一事件,
            count(DISTINCT repo_name)                             AS 仓库数,
            count(DISTINCT actor_login)                           AS 用户数,
            count(DISTINCT dt)                                    AS 覆盖天数,
            min(created_at)                                       AS 最早,
            max(created_at)                                       AS 最晚
        FROM read_parquet('{glob}')
    """)[0]
    print(f"\n总行数      {rows[0]:>12,}")
    print(f"唯一事件    {rows[1]:>12,}   （唯一率 {rows[1]/rows[0]*100:.2f}%）")
    print(f"仓库数      {rows[2]:>12,}")
    print(f"用户数      {rows[3]:>12,}")
    print(f"覆盖天数    {rows[4]:>12,}")
    print(f"时间范围    {rows[5]}  ~  {rows[6]}")

    print(f"\n{'规则':<26}{'结果':<14}判定")
    print("-" * 62)

    def rule(name: str, value, ok: bool, fmt: str = "{}"):
        mark = "通过" if ok else "异常"
        print(f"{name:<26}{fmt.format(value):<14}{mark}")

    nulls = q(con, f"""
        SELECT
            sum(CASE WHEN event_id IS NULL OR event_id = '' THEN 1 ELSE 0 END),
            sum(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 ELSE 0 END),
            sum(CASE WHEN repo_name IS NULL OR repo_name = '' THEN 1 ELSE 0 END),
            sum(CASE WHEN created_at IS NULL OR created_at = '' THEN 1 ELSE 0 END)
        FROM read_parquet('{glob}')
    """)[0]
    rule("R1 event_id 空值", nulls[0], nulls[0] == 0, "{:,}")
    rule("R1 event_type 空值", nulls[1], nulls[1] == 0, "{:,}")
    rule("R1 repo_name 空值", nulls[2], nulls[2] == 0, "{:,}")
    rule("R1 created_at 空值", nulls[3], nulls[3] == 0, "{:,}")

    dup = rows[0] - rows[1]
    rule("R2 event_id 重复", dup, dup / rows[0] < 0.001, "{:,}")

    expect = rows[4] * 24
    hours = q(con, f"SELECT count(DISTINCT (dt || '-' || hour)) FROM read_parquet('{glob}')")[0][0]
    rule("R3 小时分区完整", f"{hours}/{expect}", hours >= expect - 1 * rows[4], "{}")

    print(f"\n各日行数")
    for d, h, n in q(con, f"""
        SELECT dt, count(DISTINCT hour), count(*)
        FROM read_parquet('{glob}') GROUP BY 1 ORDER BY 1
    """):
        print(f"  {d}   {h:>2} 小时   {n:>10,} 行")

    print(f"\n事件类型 Top{args.top}")
    for t, n in q(con, f"""
        SELECT event_type, count(*) c FROM read_parquet('{glob}')
        GROUP BY 1 ORDER BY c DESC LIMIT {args.top}
    """):
        print(f"  {t:<34}{n:>10,}  {n/total*100:5.1f}%")

    print(f"\n活跃仓库 Top{min(args.top, 10)}")
    for r, n in q(con, f"""
        SELECT repo_name, count(*) c FROM read_parquet('{glob}')
        GROUP BY 1 ORDER BY c DESC LIMIT {min(args.top, 10)}
    """):
        print(f"  {r:<40}{n:>8,}")

    avg_payload = q(con, f"SELECT avg(length(payload)) FROM read_parquet('{glob}')")[0][0]
    print(f"\n单条平均体积  {avg_payload:.0f} 字节 (payload)")
    print("=" * 62)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
