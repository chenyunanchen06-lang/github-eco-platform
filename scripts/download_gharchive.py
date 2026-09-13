"""
01 · 下载 GitHub Archive 小时级原始数据

输入：https://data.gharchive.org/{YYYY-MM-DD}-{H}.json.gz
输出：data/raw/dt=YYYY-MM-DD/HH.json.gz

特性：
    并发下载、全局限速（防 403 限流）、指数退避重试、断点续传（已存在文件跳过）、
    404 视为"官方该小时无数据"而非错误。

实测规模（2026-09-12）：单文件约 55 MB，单日 24 个文件约 1.3 GB，单周约 9 GB。

用法：
    python scripts/download_gharchive.py                      # 最近 7 个完整自然日
    python scripts/download_gharchive.py --days 1             # 最近 1 天（约 1.3 GB）
    python scripts/download_gharchive.py --dates 2026-09-01   # 指定日期
    python scripts/download_gharchive.py --days 1 --hours 0,1,2   # 小样验证，约 166 MB
"""
from __future__ import annotations

import argparse
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C


class RateLimiter:
    """跨线程的全局限速器：保证相邻两次请求的发起间隔不小于 min_interval 秒。

    GitHub Archive 的存储桶会对高频访问返回 403，实测连续请求会被间歇性拦截，
    因此这里主动限速，而不是靠失败后重试来兜。
    """

    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self.min_interval
        if delay > 0:
            time.sleep(delay)


def target_path(date: str, hour: int) -> Path:
    return C.RAW_DIR / f"dt={date}" / f"{hour:02d}.json.gz"


def fetch_one(date: str, hour: int, limiter: RateLimiter) -> dict:
    """下载单个小时文件。返回结果字典，不抛异常。"""
    dst = target_path(date, hour)

    if dst.exists() and dst.stat().st_size > 0:
        return {"date": date, "hour": hour, "status": "skip", "size": dst.stat().st_size}

    # 注意：GH Archive 文件名的小时位不补零，正确写法是 2026-09-01-0.json.gz，
    # 写成 -00.json.gz 会返回 404。这里必须用 {hour} 而不是 {hour:02d}。
    url = f"{C.ARCHIVE_BASE}/{date}-{hour}.json.gz"
    dst.parent.mkdir(parents=True, exist_ok=True)
    last_err = ""

    for attempt in range(1, C.RETRY + 1):
        limiter.wait()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": C.USER_AGENT})
            with urllib.request.urlopen(req, timeout=C.TIMEOUT) as resp:
                tmp = dst.with_name(dst.name + ".part")
                with open(tmp, "wb") as fh:
                    while True:
                        chunk = resp.read(1 << 20)
                        if not chunk:
                            break
                        fh.write(chunk)
                tmp.replace(dst)
            return {"date": date, "hour": hour, "status": "ok", "size": dst.stat().st_size}
        except urllib.error.HTTPError as exc:
            # 404 = 官方该小时确实没有数据，属正常，不重试
            if exc.code == 404:
                return {"date": date, "hour": hour, "status": "missing", "size": 0}
            # 403 = 被限流，退避后重试（实测高频访问会间歇性触发）
            last_err = f"HTTP {exc.code}" + ("（限流）" if exc.code == 403 else "")
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
        # 指数退避 + 抖动，避免多个线程同时重试造成二次限流
        time.sleep(2 ** attempt + random.uniform(0, 1.5))

    return {"date": date, "hour": hour, "status": "fail", "size": 0, "error": last_err}


def main() -> int:
    ap = argparse.ArgumentParser(description="下载 GitHub Archive 原始数据")
    ap.add_argument("--days", type=int, default=7, help="最近 N 个完整自然日（默认 7）")
    ap.add_argument("--dates", type=str, default="", help="显式日期列表，逗号分隔")
    ap.add_argument("--hours", type=str, default="", help="只下指定小时，逗号分隔，如 0,1,2（默认全部 24 小时）")
    ap.add_argument("--sleep", type=float, default=0.8, help="请求最小间隔秒数，防限流（默认 0.8）")
    args = ap.parse_args()

    dates = [d.strip() for d in args.dates.split(",") if d.strip()] or C.default_dates(args.days)
    hours = [int(h) for h in args.hours.split(",") if h.strip()] or C.HOURS

    jobs = [(d, h) for d in dates for h in hours]
    limiter = RateLimiter(args.sleep)

    print(f"目标：{len(dates)} 天 x {len(hours)} 小时 = {len(jobs)} 个文件")
    print(f"日期范围：{dates[0]} ~ {dates[-1]}")
    print(f"落盘目录：{C.RAW_DIR}")
    print(f"并发度：{C.MAX_WORKERS}，请求最小间隔：{args.sleep}s，单文件超时：{C.TIMEOUT}s")
    print("-" * 52)

    started = time.time()
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=C.MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_one, d, h, limiter): (d, h) for d, h in jobs}
        for i, fut in enumerate(as_completed(futures), 1):
            results.append(fut.result())
            if i % 12 == 0 or i == len(jobs):
                print(f"  进度 {i}/{len(jobs)}")

    ok = [r for r in results if r["status"] == "ok"]
    skip = [r for r in results if r["status"] == "skip"]
    missing = [r for r in results if r["status"] == "missing"]
    failed = [r for r in results if r["status"] == "fail"]

    total_mb = sum(r["size"] for r in results) / 1024 / 1024
    print("-" * 52)
    print(f"新下载 {len(ok)} · 已存在 {len(skip)} · 官方缺失 {len(missing)} · 失败 {len(failed)}")
    print(f"原始数据合计 {total_mb:.1f} MB，耗时 {time.time() - started:.1f}s")

    if missing:
        print("\n官方无数据的小时（属正常，GH Archive 偶有空档）：")
        for r in missing[:10]:
            print(f"  {r['date']}-{r['hour']:02d}")

    if failed:
        print("\n下载失败：")
        for r in failed:
            print(f"  {r['date']}-{r['hour']:02d}  {r.get('error')}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
