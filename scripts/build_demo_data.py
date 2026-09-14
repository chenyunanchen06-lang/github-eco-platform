"""
10 · 构建演示数据集（可随仓库分发）

产出：demo/demo.duckdb   几 MB，物化好的少量表

【为什么需要它】
    data/ 和 warehouse.duckdb 都在 .gitignore 里 —— 完整数据 15 GB，不可能进仓库。
    结果是：任何人 clone 这个仓库都跑不起看板，必须先下 7.4 GB 数据 + 跑一遍 Spark。
    这对一个简历项目是硬伤：面试官不会为了看你的项目等半小时。

    所以额外产出一份「够用就好」的演示数据：
      · 概览指标全量保留（8 天的汇总行）
      · 事件类型 / PR 漏斗 / 小时热力 全量保留（行数本来就少）
      · 仓库榜 / 用户榜 / 组织榜 每天只留 Top N（看板本来也只展示 Top N）

    看板会优先用完整 warehouse.duckdb，找不到时自动回落到 demo.duckdb。

用法：
    python scripts/build_demo_data.py                 # 默认 Top 300
    python scripts/build_demo_data.py --top 500
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

DEMO_DIR = C.ROOT / "demo"
DEMO_DB = DEMO_DIR / "demo.duckdb"

# 全量保留的表（行数本来就小）
FULL_TABLES = ["daily_event_type", "daily_pr_funnel", "hourly_activity"]
# 每天只留 Top N 的排行榜
TOPN_TABLES = ["daily_repo_rank", "daily_actor_active", "daily_org_rank"]


def main() -> int:
    ap = argparse.ArgumentParser(description="构建演示数据集")
    ap.add_argument("--top", type=int, default=300, help="每个排行榜每天保留多少行")
    args = ap.parse_args()

    if not C.WAREHOUSE_DB.exists():
        print(f"找不到 {C.WAREHOUSE_DB}，请先运行 scripts/build_duckdb.py")
        return 1

    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    if DEMO_DB.exists():
        DEMO_DB.unlink()

    # 跨库读必须 ATTACH —— 直接写 src.ads.xxx 会报 Catalog "src" does not exist。
    dst = duckdb.connect(str(DEMO_DB))
    dst.execute(f"ATTACH '{C.WAREHOUSE_DB.as_posix()}' AS src (READ_ONLY)")

    print("=" * 62)
    print(f"构建演示数据集 → {DEMO_DB.relative_to(C.ROOT)}")
    print("=" * 62)

    # ── 概览指标：从事实表物化成一张小表，看板指标卡直接读它 ──
    dst.execute("""
        CREATE TABLE summary AS
        SELECT dt AS date,
               count(*)                 AS events,
               count(DISTINCT repo_id)  AS repos,
               count(DISTINCT actor_id) AS actors
        FROM read_parquet(?)
        GROUP BY 1 ORDER BY 1
    """, [(C.DWD_DIR / "gh_events_detail" / "**" / "*.parquet").as_posix()])
    n = dst.execute("SELECT count(*) FROM summary").fetchone()[0]
    print(f"  summary                {n:>8,} 行   （概览指标，全量）")

    # 【关键】必须建和完整库同名的 ads schema。
    # 看板的查询写的是 `ads.daily_repo_rank`，Text-to-SQL 模板也是。
    # 演示库如果把这些表放在默认的 main schema，切到演示模式就会全部报表不存在。
    dst.execute("CREATE SCHEMA IF NOT EXISTS ads")

    # ── 小表全量搬运 ──
    for name in FULL_TABLES:
        dst.execute(f"CREATE TABLE ads.{name} AS SELECT * FROM src.ads.{name}")
        n = dst.execute(f"SELECT count(*) FROM ads.{name}").fetchone()[0]
        print(f"  ads.{name:<18} {n:>8,} 行   （全量）")

    # ── 排行榜每天留 Top N ──
    for name in TOPN_TABLES:
        dst.execute(f"""
            CREATE TABLE ads.{name} AS
            SELECT * FROM (
                SELECT *, row_number() OVER (PARTITION BY date ORDER BY event_cnt DESC) AS _rn
                FROM src.ads.{name}
            ) WHERE _rn <= {args.top}
        """)
        dst.execute(f"ALTER TABLE ads.{name} DROP COLUMN _rn")
        total = dst.execute(f"SELECT count(*) FROM src.ads.{name}").fetchone()[0]
        n = dst.execute(f"SELECT count(*) FROM ads.{name}").fetchone()[0]
        print(f"  ads.{name:<18} {n:>8,} 行   （完整版 {total:,} 行，"
              f"每天留 Top{args.top}，压缩到 {n / max(total, 1) * 100:.1f}%）")

    # ── 元信息，让看板能标注这是抽样数据 ──
    dst.execute("""
        CREATE TABLE meta AS
        SELECT current_timestamp AS built_at,
               (SELECT count(*) FROM summary) AS days,
               (SELECT min(date) FROM summary) AS date_from,
               (SELECT max(date) FROM summary) AS date_to,
               'demo' AS kind
    """)
    n = dst.execute("SELECT * FROM meta").fetchone()
    dst.execute("DETACH src")
    dst.close()

    size = DEMO_DB.stat().st_size / 1024 / 1024
    print("-" * 62)
    print(f"覆盖 {n[4]} 天（{n[2]} ~ {n[3]}）")
    print(f"demo.duckdb  {size:.2f} MB   —— 可以随仓库提交")
    print("=" * 62)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
