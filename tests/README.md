# tests 目录说明

这里是 TickForge 的测试集，当前 **276 passed**。测试不连接真实 Binance：单元测试用纯函数/本地数据结构，集成测试用本地 WebSocket + REST 交易所替身；真实公网行情由 `live` 运行单独验证。

## 1. 怎么跑

```bash
python -m pip install -r requirements-dev-lock.txt
python -m pytest -q
```

预期输出：`276 passed`。测试通常 8–10 秒跑完，不需要 API Key，也不访问外网。

## 2. 测试分层

| 层级 | 文件 | 重点 |
|---|---|---|
| 单元 | `test_book.py`、`test_wal.py`、`test_normalize.py`、`test_factors.py`、`test_strategy.py`、`test_simulator.py`、`test_config.py`、`test_derived.py`、`test_metrics.py`、`test_health.py`、`test_trade_quality.py` | 每个模块的边界、公式、序列规则、确定性和健康滞后口径 |
| 集成 | `test_integration_live.py`、`test_feed.py`、`test_rest.py`、`test_http.py` | 本地交易所替身 → live 引擎 → WAL → replay；WebSocket 重连、REST 重试、HTTP 健康接口 |
| 变体/执行语义 | `test_spot_long_only.py`、`test_simulator.py` | 现货、现金/持仓约束、基于 T 的 quote-time guard |
| 交付安全 | `test_verify_fail_closed.py`、`test_replay_config.py` | 缺 summary/manifest、坏 derived、缺失 config 时必须失败，不能静默回退 |

## 3. 各文件覆盖什么

| 文件 | 主要内容 |
|---|---|
| `test_book.py` | Binance `U/u` 同步、快照 `lastUpdateId` 边界、重复/重叠/gap、缓冲溢出、交叉簿、`top_levels` 排序；还有随机事件流对拍“实现 vs 朴素全量簿” |
| `test_wal.py` | 10k 记录往返、分段轮转、CRC 篡改、尾部截断、fsync 策略、关闭/digest、seq 单调和长度上限 |
| `test_normalize.py` | 价格/数量转整数 tick、非法输入、depth/aggTrade/bookTicker 解析、快照编解码、坏 JSON/字段缺失 |
| `test_factors.py` | microprice/spread 手算、OFI 四种最优档变化、成交不平衡、动量窗口、波动非负、节流/乱序、reset、无簿/单边簿 |
| `test_strategy.py` | 公允价、库存偏移方向、冷却、单边限制、`round_to_tick` 向下/向上取整 |
| `test_simulator.py` | 买卖成交、部分成交、费率、平均成本、名义上限、取消、summary 对账、重复运行 Fill 序列一致、成交早于报价不成交 |
| `test_spot_long_only.py` | `allow_short=false` 时不裸卖、卖出不超过持仓、买入受现金限制、期末仓位 |
| `test_config.py` | 默认值、TOML 覆盖、环境变量覆盖、非法值拒绝、stream 顺序 |
| `test_derived.py` | factors/signals/fills JSONL sink 的确定性写入和哈希输入 |
| `test_metrics.py` | 计数器、直方图分位、Prometheus 文本、并发访问 |
| `test_health.py` | 处理队列最老待处理事件的等待时间；健康取到达差值与队列头等待的较大者 |
| `test_http.py` | `/healthz`、`/readyz`、`/status`、`/metrics` 的 200/503 行为（本地真实 socket） |
| `test_feed.py` | 本地 WebSocket 的接收时间戳、断线重连、stale 超时 |
| `test_rest.py` | REST 快照成功、500 重试、重试耗尽、字段校验失败 |
| `test_integration_live.py` | 本地模拟交易所 → 实时引擎 → WAL → 派生输出 → 回放摘要一致的端到端链路 |
| `test_replay_config.py` | `config.json` 缺失/损坏时 replay 不能静默使用默认配置 |
| `test_trade_quality.py` | 按 symbol 检查 `aggTrade.a` 重复、回退、gap、缺失 ID |
| `test_verify_fail_closed.py` | 空目录、损坏 summary/manifest、被改过的 derived 必须 `ok=false` |

## 4. 阅读建议

- 想理解订单簿：先看 `test_book.py`，里面的随机对拍最能说明状态机规则；
- 想理解确定性：看 `test_simulator.py`、`test_factors.py`、`test_derived.py`；
- 想理解交付校验：看 `test_verify_fail_closed.py` 和 `test_replay_config.py`；
- 想理解完整链路：看 `test_integration_live.py`。

更完整的测试矩阵、故障注入和异常终止实验见 `docs/正确性与可靠性验证.md`。
