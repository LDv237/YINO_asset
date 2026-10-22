# 主结果：20261021T162501Z-9821

本目录为 15 分钟**现货 live** run，窗口为 2026-10-21 16:25:05–16:40:05 UTC（北京时间 2026-10-22 00:25:05–00:40:05）；日志、signals、fills、性能与回放均取自同一次运行，口径一致。

> 目录名 `01_主结果_现货_9821` 里的 `9821` 是 run_id 简写；完整 run_id 是 `20261021T162501Z-9821`。`tickforge verify` 输出的 `run_id` 字段取目录名，内部 `summary.json` / `manifest.json` 的 run_id 是完整值。

## 关键结果

| 指标 | 值 |
|---|---:|
| 原始帧 / WAL 记录 | 650,532 / 650,542（10 个快照） |
| 信号 / fills | 47,127 / 6,437 |
| 原始帧丢失 / clean_shutdown | 0 / true |
| data gap / 订单簿 resync | 0 / 0 |
| 负仓位 / 负现金 symbol 数 | 0 / 0 |
| depth e2e p50 / p95 / p99 / p999 | 0.609 / 1.722 / 2.896 / 4.871 ms |
| 聚合 e2e p50 / p99 / p999 | 0.724 / 2.435 / 5.793 ms（bookTicker 约占 86.0%） |
| wal_queue_max / pipeline_queue_max | 735 / 16 |
| recv→fsync p50 / p95（批次） | 262.144 / 311.744 ms（`interval` 批次口径） |
| 窗口 / 平均速率 | 900.037 s / 722.8 帧/s |
| 起始小时桶活跃度 | 16:00–17:00 UTC，69.42M USDT / 194,279 笔，排 9/47（82.98 分位） |

## 文件

```text
run_data.tar.xz      raw/ 和 derived/（解压后直接 verify/replay）
summary.json         计数、延迟分位、资源、对账、digests
manifest.json        WAL 分段和 derived 的 SHA-256
config.json          本次运行配置（spot_long_only）
replay_summary.json  replay --verify 结果
logs/tickforge.log   结构化日志
console.log          控制台日志
```

## 复现

```bash
python scripts/extract_main_run.py 运行材料/01_主结果_现货_9821
python -m tickforge verify --run 运行材料/01_主结果_现货_9821
python -m tickforge replay --run 运行材料/01_主结果_现货_9821 \
  --out 运行材料/01_主结果_现货_9821-replay --verify
```

预期 `verify` 返回 `ok=true`，`replay --verify` 返回 `verified=true`、`deterministic=true`，三个 SHA-256 一致。
