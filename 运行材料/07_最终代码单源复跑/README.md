# 最终代码单源复跑

run_id: `20261021T075925Z-3049`

这个目录保留 2026-10-21 用最终代码跑的 15 分钟 run。窗口为 07:59:29–08:14:29 UTC，跨 07:00–08:00 和 08:00–09:00 UTC 两个活跃小时桶；日志、signals、fills、性能和回放都来自同一次运行，没有 live/重算混合口径。

## 关键结果

| 指标 | 值 |
|---|---:|
| 原始帧 / WAL 记录 | 303,784 / 303,793 |
| 信号 / fills | 41,470 / 8,828 |
| 原始帧丢失 / clean_shutdown | 0 / True |
| data gap / 订单簿 resync | 0 / 0 |
| depth e2e p50 / p95 / p99 / p999 | 0.512 / 1.448 / 2.435 / 4.871 ms |
| 聚合 e2e p50 / p99 / p999 | 0.512 / 2.435 / 5.793 ms（bookTicker 占 80.2%） |
| wal_queue_max / pipeline_queue_max | 413 / 16 |
| recv→fsync p50 / p95 | 262.144 / 311.744 ms（`interval` 批次口径） |
| 窗口 / 平均速率 | 900.129 s / 337.5 msg/s |

## 复现

```bash
python scripts/reproduce_from_delivery.py --delivery 运行材料/07_最终代码单源复跑
```

脚本会解压 `原始WAL.tar.gz` 和 `derived.tar.gz`，复制 config/summary/manifest，然后执行 `verify` 和 `replay --verify`。预期 `verify` 返回 `ok=true`，`replay --verify` 返回 `verified=true`、`deterministic=true`，三个 SHA-256 一致。
