# tools 目录说明

这里的脚本是**运行后分析和复现辅助工具**，不属于 live 主链路。它们读取 `artifacts/runs/<run_id>/` 里的 WAL、derived、summary、manifest，或从 WAL 生成可读视图、样本和验证证据。

## 1. 工具列表

| 工具 | 作用 | 典型命令 |
|---|---|---|
| `analyze_run.py` | 汇总一次运行的延迟、计数、资源和一致性，供性能报告使用 | `python tools/analyze_run.py artifacts/runs/<run_id>` |
| `wal_export.py` | 把二进制 WAL 导出为人类可读 JSONL（可 gzip），保留 seq、接收时间和原始 payload | `python tools/wal_export.py --run artifacts/runs/<run_id> --out artifacts/exports/raw.jsonl.gz` |
| `wal_export_csv.py` | 按事件类型导出 CSV：`agg_trades.csv`、`book_ticker.csv`、`depth_updates.csv`、`snapshots.csv` | `python tools/wal_export_csv.py --run artifacts/runs/<run_id> --out-dir artifacts/exports/csv` |
| `trim_run.py` | 从一次 live run 的 WAL 裁出小样本，保持 `raw/` + `config.json` 的 run 布局，便于离线 replay | `python tools/trim_run.py --run artifacts/runs/<run_id> --out samples/demo_run --records 20000` |
| `make_sample_reference.py` | 给离线样本生成参考 `summary.json`，让样本也能走 `replay --verify` 的比较路径 | `python tools/make_sample_reference.py --sample samples/demo_run` |
| `replay_spot_long_only.py` | 用同一份 WAL 做受控对照：只切换现货只做多执行语义，比较 fills/仓位/现金 | `python tools/replay_spot_long_only.py --run artifacts/runs/<run_id> --out artifacts/experiments/variants/spot_long_only_same_wal_final` |
| `trace_signal.py` | 按 `(symbol, seq)` 追溯一条信号到因子快照和 WAL 原始帧，并说明 fills 的 `trade_id` 链接 | `python tools/trace_signal.py --run artifacts/runs/<run_id> --index 0` |
| `market_activity.py` | 拉 Binance 小时 K 线，给运行窗口所在小时桶做成交额/笔数排名 | `python tools/market_activity.py --target 2026-10-21T07:59:25Z --out artifacts/evidence/market_activity.json` |
| `crash_recovery_check.py` | 起一个短时 live、强制杀进程，再恢复读取 WAL 前缀，检查记录数、CRC/seq 和残缺尾部；该检查不证明哪些记录已完成 fsync | `python tools/crash_recovery_check.py --help` |

## 2. 使用顺序

常见的一条链：

```bash
RUN=运行材料/20261021T075925Z-3049

# 1) 看运行摘要
python tools/analyze_run.py "$RUN"

# 2) 导出人类可读行情
python tools/wal_export.py --run "$RUN" --out artifacts/exports/raw.jsonl.gz
python tools/wal_export_csv.py --run "$RUN" --out-dir artifacts/exports/csv

# 3) 追溯一条信号
python tools/trace_signal.py --run "$RUN" --index 0

# 4) 同一 WAL 的只做多对照
python tools/replay_spot_long_only.py --run "$RUN" \
  --out artifacts/experiments/variants/spot_long_only_same_wal_final
```

## 3. 边界

- `wal_export*.py` 只读 WAL，不修改原始数据；
- `trim_run.py` 和 `make_sample_reference.py` 会写输出目录；对已有目录先看 `--help`，不要覆盖正式 run；
- `replay_spot_long_only.py` 不比较参考 run 的派生 digest，因为执行语义变了，signals/fills 本来就应该不同；
- `crash_recovery_check.py` 是应用层强制终止测试，不等同于掉电或物理介质测试；
- `market_activity.py` 只使用已完成的整小时 K 线，运行窗口落在未完成小时时无法排名。

正式运行、回放和校验命令见 `docs/运行手册.md`；指标和延迟口径见 `docs/性能实测.md`。
