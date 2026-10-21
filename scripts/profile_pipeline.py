"""
Профилирование подходов (MapReduce и DuckDB): время и пик памяти, N запусков каждого.

Запуск из корня проекта:
    python scripts/profile_pipeline.py                  # оба подхода, 3 запуска
    python scripts/profile_pipeline.py --runs 5
    python scripts/profile_pipeline.py --only duckdb

Время  - от старта подпроцесса до его завершения (включая чтение из RustFS и запись результатов).
Память - пик суммарного RSS процесса и всех его дочерних процессов (пул мапперов),
         замер каждые 50 мс.
"""
import argparse
import csv
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "profiling" / "logs"
OUT_CSV = ROOT / "profiling" / "profiling_runs.csv"
OUT_TXT = ROOT / "reports" / "output" / "stage7_profiling.txt"
SAMPLE_INTERVAL = 0.05  # секунды

# Команды запуска (из корня проекта). Это обычные боевые запуски этапа 3 (с загрузкой
# результатов в S3, без инъекции отказа).
# MapReduce пишет результаты с суффиксом _prof и свой транскрипт в отдельный файл, чтобы не
# затирать результаты и транскрипт этапа 3, по которым делалась верификация.
# run_duckdb.py флагов не имеет: он перезапишет results/duckdb и stage3_duckdb.txt
# теми же данными (меняются только времена в транскрипте).
TARGETS = {
    "mapreduce": [sys.executable, "src/mapreduce/main.py",
                  "--tag", "_prof", "--out", "reports/output/stage7_mapreduce_run.txt"],
    "duckdb": [sys.executable, "scripts/run_duckdb.py"],
}


class PeakMemory(threading.Thread):
    """Раз в SAMPLE_INTERVAL суммирует RSS процесса и всех его потомков, помнит максимум."""

    def __init__(self, pid):
        super().__init__(daemon=True)
        self.proc = psutil.Process(pid)
        self.peak = 0
        self._done = threading.Event()

    def _total(self):
        try:
            procs = [self.proc] + self.proc.children(recursive=True)
        except psutil.NoSuchProcess:
            return 0
        total = 0
        for p in procs:
            try:
                total += p.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return total

    def run(self):
        while not self._done.is_set():
            self.peak = max(self.peak, self._total())
            self._done.wait(SAMPLE_INTERVAL)

    def stop(self):
        self._done.set()
        self.join()


def run_once(name, cmd, run_no):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{name}_run{run_no}.log"
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    with open(log_path, "w", encoding="utf-8") as log:
        start = time.perf_counter()
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env=env)
        mem = PeakMemory(proc.pid)
        mem.start()
        code = proc.wait()
        elapsed = time.perf_counter() - start
        mem.stop()
    if code != 0:
        sys.exit(f"[{name}] запуск {run_no} завершился с кодом {code}, лог: {log_path}")
    return elapsed, mem.peak / 2**20


def mean_std(values):
    return statistics.mean(values), (statistics.stdev(values) if len(values) > 1 else 0.0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--runs", type=int, default=3, help="число запусков каждого подхода (>= 3)")
    ap.add_argument("--only", nargs="+", choices=list(TARGETS), help="профилировать только эти подходы")
    args = ap.parse_args()
    names = args.only or list(TARGETS)

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    report = open(OUT_TXT, "w", encoding="utf-8")

    def log(line=""):
        print(line, flush=True)
        report.write(line + "\n")
        report.flush()

    log(f"Профилирование: {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"Система: {platform.platform()}, CPU (логических): {os.cpu_count()}, "
        f"RAM: {psutil.virtual_memory().total / 2**30:.1f} ГБ, Python {platform.python_version()}")
    for name in names:
        log(f"Команда [{name}]: {' '.join(TARGETS[name])}")
    log(f"Запусков на подход: {args.runs}")
    log()

    rows = []  # (подход, запуск, время, память)
    for name in names:
        for i in range(1, args.runs + 1):
            log(f"[{name}] запуск {i}/{args.runs} ...")
            seconds, mib = run_once(name, TARGETS[name], i)
            rows.append((name, i, seconds, mib))
            log(f"    время {seconds:.2f} с, пик памяти {mib:.0f} МБ")

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["approach", "run", "seconds", "peak_rss_mib"])
        w.writerows((n, i, f"{s:.3f}", f"{m:.1f}") for n, i, s, m in rows)

    log("\nВсе запуски:")
    log("| Подход | Запуск | Время, с | Пик памяти, МБ |")
    log("|---|---|---|---|")
    for n, i, s, m in rows:
        log(f"| {n} | {i} | {s:.2f} | {m:.0f} |")

    log("\nИтог (среднее ± стандартное отклонение):")
    log("| Подход | Время, с | Мин–макс, с | Пик памяти, МБ |")
    log("|---|---|---|---|")
    means = {}
    for n in names:
        t = [s for nn, _, s, _ in rows if nn == n]
        m = [x for nn, _, _, x in rows if nn == n]
        (tm, ts), (mm, ms) = mean_std(t), mean_std(m)
        means[n] = (tm, mm)
        log(f"| {n} | {tm:.2f} ± {ts:.2f} | {min(t):.2f}–{max(t):.2f} | {mm:.0f} ± {ms:.0f} |")

    if "mapreduce" in means and "duckdb" in means:
        (t_mr, m_mr), (t_db, m_db) = means["mapreduce"], means["duckdb"]
        log(f"\nMapReduce / DuckDB: по времени в {t_mr / t_db:.1f} раза, по пику памяти в {m_mr / m_db:.1f} раза")

    log(f"\nCSV: {OUT_CSV.relative_to(ROOT)}, логи запусков: {LOG_DIR.relative_to(ROOT)}")
    report.close()


if __name__ == "__main__":
    main()