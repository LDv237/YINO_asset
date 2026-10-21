# 主结果：20261021T075925Z-3049

这个目录保留 2026-10-21 用最终代码跑的 15 分钟 live run。窗口为 07:59:29–08:14:29 UTC；日志、signals、fills、性能和回放都来自同一次运行，没有 live/重算混合口径。

## 关键结果

| 指标 | 值 |
|---|---:|
| 原始帧 / WAL 记录 | 303,784 / 303,793 |
| 信号 / fills | 41,470 / 8,828 |
| 原始帧丢失 / clean_shutdown | 0 / true |
| data gap / 订单簿 resync | 0 / 0 |
| depth e2e p50 / p95 / p99 / p999 | 0.512 / 1.448 / 2.435 / 4.871 ms |
| 聚合 e2e p50 / p99 / p999 | 0.512 / 2.435 / 5.793 ms（bookTicker 占 80.2%） |
| wal_queue_max / pipeline_queue_max | 413 / 16 |
| recv→fsync p50 / p95 | 262.144 / 311.744 ms（`interval` 批次口径） |
| 窗口 / 平均速率 | 900.129 s / 337.5 msg/s |

## 文件

```text
raw/                 2 个 WAL 分段（303,793 条记录）
derived/             factors / signals / fills JSONL
config.json          本次运行的完整配置
summary.json         计数、延迟分位、资源、对账、digests
manifest.json        WAL 分段和 derived 文件的 SHA-256
replay_summary.json  replay --verify 结果
logs/tickforge.log   结构化日志
console.log          控制台日志
```

## 复现

目录已经是 `tickforge verify` 能直接读取的 run 布局，不需要解压或重建：

```bash
python -m tickforge verify --run 运行材料/20261021T075925Z-3049
python -m tickforge replay --run 运行材料/20261021T075925Z-3049 \
  --out 运行材料/20261021T075925Z-3049-replay --verify
```

预期 `verify` 返回 `ok=true`，`replay --verify` 返回 `verified=true`、`deterministic=true`，三个 SHA-256 一致。
