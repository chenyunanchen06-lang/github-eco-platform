"""
02 · 原始 gz → ODS Parquet

输入：data/raw/dt=YYYY-MM-DD/HH.json.gz
输出：data/ods/dt=YYYY-MM-DD/hour=HH/part-0.parquet

设计说明：
  ODS 只展开事件顶层字段，payload 以 JSON 字符串原样保留。
  好处是 ODS 体积远小于原始 JSON，后续 DWD 层做 payload 二次解析时
  不必再回到 gz 文件重读 —— 这是"原始层一次落地，多次消费"的常规做法。

用法：
    python scripts/raw_to_ods.py
    python scripts/raw_to_ods.py --overwrite
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import zlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

ODS_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("actor_id", pa.int64()),
    ("actor_login", pa.string()),
    ("repo_id", pa.int64()),
    ("repo_name", pa.string()),
    ("org_id", pa.int64()),
    ("org_login", pa.string()),
    ("created_at", pa.string()),
    ("is_public", pa.bool_()),
    ("payload", pa.string()),
])


def parse_line(line: str) -> dict | None:
    """把一行 JSON 事件转成 ODS 记录；无法解析返回 None。"""
    line = line.strip()
    if not line:
        return None
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        return None

    actor = ev.get("actor") or {}
    repo = ev.get("repo") or {}
    org = ev.get("org") or {}
    payload = ev.get("payload")

    return {
        "event_id": str(ev.get("id") or ""),
        "event_type": ev.get("type") or "",
        "actor_id": actor.get("id"),
        "actor_login": actor.get("login") or "",
        "repo_id": repo.get("id"),
        "repo_name": repo.get("name") or "",
        "org_id": org.get("id"),
        "org_login": org.get("login"),
        "created_at": ev.get("created_at") or "",
        "is_public": ev.get("public"),
        "payload": (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            if payload is not None else None
        ),
    }


def convert_file(gz_path: Path, out_dir: Path, overwrite: bool) -> dict:
    out_file = out_dir / "part-0.parquet"
    if out_file.exists() and not overwrite:
        with pq.ParquetFile(out_file) as pf:
            return {"rows": pf.metadata.num_rows, "bad": 0, "bytes": out_file.stat().st_size, "reused": True}

    rows: list[dict] = []
    bad = 0
    try:
        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                rec = parse_line(line)
                if rec is None:
                    bad += 1
                else:
                    rows.append(rec)
    except (EOFError, OSError, gzip.BadGzipFile, zlib.error) as exc:
        # 下载被中断时 gz 会截断，表现为「Compressed file ended before the
        # end-of-stream marker」。**不要让它炸掉整个批次** —— 记下来，让调用方
        # 汇总后提示用户重下这一个文件。
        return {"rows": 0, "bad": bad, "bytes": 0, "reused": False,
                "error": f"{type(exc).__name__}: {exc}"}

    if not rows:
        return {"rows": 0, "bad": bad, "bytes": 0, "reused": False}

    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=ODS_SCHEMA)
    pq.write_table(table, out_file, compression="zstd")
    return {"rows": len(rows), "bad": bad, "bytes": out_file.stat().st_size, "reused": False}


def main() -> int:
    ap = argparse.ArgumentParser(description="原始 gz 转 ODS Parquet")
    ap.add_argument("--overwrite", action="store_true", help="重跑已生成的 Parquet")
    args = ap.parse_args()

    day_dirs = sorted(p for p in C.RAW_DIR.glob("dt=*") if p.is_dir())
    if not day_dirs:
        print(f"{C.RAW_DIR} 下没有原始数据，请先运行 scripts/download_gharchive.py")
        return 1

    grand_rows = grand_bad = grand_bytes = grand_raw_bytes = 0
    processed = reused = 0
    broken: list[str] = []

    for day_dir in day_dirs:
        date = day_dir.name.split("=", 1)[1]
        gz_files = sorted(day_dir.glob("*.json.gz"))
        if not gz_files:
            continue

        day_rows = 0
        print(f"[{date}] {len(gz_files)} 个小时文件")
        for gz in gz_files:
            hour = int(gz.stem.split(".")[0])
            out_dir = C.ODS_DIR / f"dt={date}" / f"hour={hour:02d}"
            stat = convert_file(gz, out_dir, args.overwrite)

            if stat.get("error"):
                broken.append(f"{gz.as_posix()}  ({stat['error']})")
                print(f"          ⚠️ 跳过损坏文件 {gz.name}: {stat['error']}")
                continue

            day_rows += stat["rows"]
            grand_rows += stat["rows"]
            grand_bad += stat["bad"]
            grand_bytes += stat["bytes"]
            grand_raw_bytes += gz.stat().st_size
            processed += 1
            reused += 1 if stat.get("reused") else 0

        print(f"          {day_rows:,} 条事件")

    print("-" * 52)
    print(f"处理文件 {processed} 个（复用 {reused} 个）")
    if broken:
        print(f"\n⚠️  {len(broken)} 个文件损坏（多半是下载被中断导致 gz 截断）。")
        print("   删掉它们再重跑下载即可自动补回：")
        for b in broken:
            print(f"     - {b}")
    print(f"事件总行数   {grand_rows:,}")
    print(f"解析失败行   {grand_bad:,}")
    if grand_bytes:
        print(f"原始 gz      {grand_raw_bytes / 1024 / 1024:.1f} MB")
        print(f"ODS Parquet  {grand_bytes / 1024 / 1024:.1f} MB"
              f"（压缩比 {grand_raw_bytes / grand_bytes:.1f}x）")
        print(f"单条平均     {grand_bytes / max(grand_rows, 1):.0f} 字节")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
