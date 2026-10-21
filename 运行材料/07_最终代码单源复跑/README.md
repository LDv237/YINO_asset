# 最终代码单源复跑

run_id: `20261021T055305Z-af19`

这个目录保留 2026-10-21 用最终代码跑的 15 分钟 run。日志、signals、fills、性能和回放都来自同一次运行，没有 live/重算混合口径。

## 关键结果

| 指标 | 值 |
|---|---:|
| 原始帧 / WAL 记录 | 244,570 / 244,582 |
| 信号 / fills | 39,716 / 6,370 |
| 原始帧丢失 / clean_shutdown | 0 / True |
| data gap / resync | 0 / 0 |
| e2e p50 / p99 / p999 | 0.431 / 2.435 / 5.793 ms |
| wal_queue_max / pipeline_queue_max | 303 / 18 |
| recv→fsync p50 / p95 | 262.144 / 311.744 ms |

## 复现

```bash
python scripts/reproduce_from_delivery.py --delivery 交付材料/07_最终代码单源复跑
```

脚本会解压 `原始WAL.tar.gz` 和 `derived.tar.gz`，复制 config/summary/manifest，然后执行 `verify` 和 `replay --verify`。
