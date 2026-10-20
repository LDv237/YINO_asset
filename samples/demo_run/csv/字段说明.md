# CSV 视图说明（离线样本）

这些 CSV 是 `samples/demo_run` 前 20,000 条 WAL 记录的视图，WAL 仍是权威原始数据。

| 文件 | 行数 | 说明 |
|---|---:|---|
| `agg_trades.csv` | 1,074 | 交易所聚合成交 |
| `book_ticker.csv` | 16,021 | 最优买/卖档更新 |
| `depth_updates.csv` | 2,899 | diff depth 事件，含重建后的 best bid/ask |
| `snapshots.csv` | 6 | REST 快照元数据 |

字段含义与正式运行相同；`seq` 可与 `raw_events.jsonl.gz` 对照。启动时快照到达前的 depth 行 `best_bid/ask` 为空，属正常现象。
