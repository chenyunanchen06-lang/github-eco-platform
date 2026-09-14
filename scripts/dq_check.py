"""
05 · 数据质量校验

用 DuckDB 直接读 Parquet（不需要 Spark），跑 5 条规则，输出：
    reports/dq_report.json   机器可读，供调度器判断是否阻断下游
    控制台表格                人工可读

5 条规则：

    R1 非空    ods.gh_events            关键字段空值 = 0
    R2 唯一    dwd.gh_events_detail     event_id 重复率 < 0.1%
    R3 完整    ods.gh_events            每日小时分区数 >= 23
    R4 波动    dwd.gh_events_detail     相邻日行数波动 < 30%
    R5 枚举    dwd.gh_events_detail     event_type 全部落在白名单内

用法：
    python scripts/dq_check.py
    python scripts/dq_check.py --date 2026-09-10      # 只校验某天
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

# ODS 的 dt 是 Hive 分区列（字符串）；DWD 的 dt 是真正的 DATE 列 —— 过滤写法不同
ODS = (f"read_parquet('{(C.ODS_DIR / '**' / '*.parquet').as_posix()}', "
       f"hive_partitioning=true)")
DWD = (f"read_parquet('{(C.DWD_DIR / 'gh_events_detail' / '**' / '*.parquet').as_posix()}', "
       f"hive_partitioning=true)")

EVENT_TYPES = [
    "PushEvent", "PullRequestEvent", "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent", "IssuesEvent", "IssueCommentEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent",
    "ReleaseEvent", "CommitCommentEvent", "PublicEvent",
    "MemberEvent", "GollumEvent",
]
VALID_EVENTS = ", ".join(f"'{e}'" for e in EVENT_TYPES)


class Report:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def add(self, rid, name, target, value, expect, passed, action, detail="") -> None:
        self.rows.append({
            "id": rid, "name": name, "target": target,
            "value": value, "expect": expect,
            "passed": bool(passed), "action": action, "detail": detail,
        })

    def show(self) -> None:
        print(f"\n{'规则':<5}{'名称':<7}{'实际值':>15}  {'期望':<16}{'判定':<9}{'失败动作'}")
        print("-" * 84)
        for r in self.rows:
            print(f"{r['id']:<5}{r['name']:<7}{str(r['value']):>15}  "
                  f"{r['expect']:<16}{'通过' if r['passed'] else '**失败**':<9}{r['action']}")
            if r["detail"]:
                print(f"       └─ {r['detail']}")

    def dump(self, path: Path) -> dict:
        doc = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "scope": "all" ,
            "summary": {
                "total": len(self.rows),
                "passed": sum(1 for r in self.rows if r["passed"]),
                "failed": sum(1 for r in self.rows if not r["passed"]),
                "blocking_failed": sum(1 for r in self.rows
                                       if not r["passed"] and r["action"] == "阻断下游"),
            },
            "rules": self.rows,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        return doc


def main() -> int:
    ap = argparse.ArgumentParser(description="数据质量校验")
    ap.add_argument("--date", type=str, default="", help="只校验指定日期，如 2026-09-10")
    args = ap.parse_args()

    ods_where = f"WHERE dt = '{args.date}'" if args.date else ""
    dwd_where = f"WHERE dt = DATE '{args.date}'" if args.date else ""

    con = duckdb.connect()
    rep = Report()

    print("=" * 84)
    print(f"数据质量校验报告{'  （仅 ' + args.date + '）' if args.date else '  （全量）'}")
    print("=" * 84)

    # ── R1 关键字段非空 ────────────────────────────────────────────────
    row = con.execute(f"""
        SELECT
          sum(CASE WHEN event_id   IS NULL OR event_id   = '' THEN 1 ELSE 0 END),
          sum(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 ELSE 0 END),
          sum(CASE WHEN created_at IS NULL OR created_at = '' THEN 1 ELSE 0 END),
          count(*)
        FROM {ODS} {ods_where}
    """).fetchone()
    bad = row[0] + row[1] + row[2]
    rep.add("R1", "非空", "ods.gh_events", f"{bad} 条空值", "= 0", bad == 0, "阻断下游",
            detail=f"共 {row[3]:,} 行 | event_id空 {row[0]} / event_type空 {row[1]} / "
                   f"created_at空 {row[2]}")

    # ── R2 event_id 唯一 ──────────────────────────────────────────────
    tot, uniq = con.execute(
        f"SELECT count(*), count(DISTINCT event_id) FROM {DWD} {dwd_where}").fetchone()
    dup = tot - uniq
    rate = dup / max(tot, 1)
    rep.add("R2", "唯一", "dwd.gh_events_detail", f"{dup:,} 条重复", "< 0.1%",
            rate < 0.001, "告警",
            detail=f"重复率 {rate * 100:.4f}% | 总数 {tot:,}，去重后 {uniq:,}")

    # ── R3 小时分区完整性 ─────────────────────────────────────────────
    hours = con.execute(
        f"SELECT dt, count(DISTINCT hour) FROM {ODS} {ods_where} GROUP BY dt ORDER BY 1"
    ).fetchall()
    worst = min(h[1] for h in hours) if hours else 0
    short = [h for h in hours if h[1] < 24]
    rep.add("R3", "完整", "ods.gh_events", f"最少 {worst}/24 小时", ">= 23",
            worst >= 23, "告警",
            detail=(f"{len(short)}/{len(hours)} 天不足 24 小时" +
                    ("：" + "、".join(f"{d}({n}h)" for d, n in short[:6]) if short else "")))

    # ── R4 日环比波动 ─────────────────────────────────────────────────
    daily = con.execute(
        f"SELECT dt, count(*) FROM {DWD} {dwd_where} GROUP BY dt ORDER BY dt").fetchall()
    worst_jump, worst_pair = 0.0, ""
    for i in range(1, len(daily)):
        prev, cur = daily[i - 1][1], daily[i][1]
        if prev:
            jump = abs(cur - prev) / prev
            if jump > worst_jump:
                worst_jump, worst_pair = jump, f"{daily[i - 1][0]}→{daily[i][0]}"
    rep.add("R4", "波动", "dwd.gh_events_detail", f"最大 {worst_jump * 100:.1f}%", "< 30%",
            worst_jump < 0.30 or len(daily) < 2, "告警",
            detail=f"最陡的一对：{worst_pair or '（天数不足，跳过）'} | 覆盖 {len(daily)} 天")

    # ── R5 event_type 枚举 ────────────────────────────────────────────
    alien = con.execute(
        f"SELECT count(*) FROM {DWD} WHERE event_type NOT IN ({VALID_EVENTS}) "
        f"{('AND dt = DATE ' + chr(39) + args.date + chr(39)) if args.date else ''}"
    ).fetchone()[0]
    rep.add("R5", "枚举", "dwd.gh_events_detail", f"{alien} 条越界", "100% 在白名单",
            alien == 0, "告警",
            detail=f"白名单 {len(EVENT_TYPES)} 种事件类型")

    rep.show()

    doc = rep.dump(C.REPORT_DIR / "dq_report.json")
    print("-" * 84)
    print(f"通过 {doc['summary']['passed']}/{doc['summary']['total']} 条  |  "
          f"阻断级失败 {doc['summary']['blocking_failed']} 条")
    print(f"报告 → {C.REPORT_DIR / 'dq_report.json'}")
    return 1 if doc["summary"]["blocking_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
