# tickforge 包说明

这是 TickForge 的核心运行时包。入口是 `python -m tickforge`（`__main__.py` → `cli.py`），支持 `live`、`replay`、`verify`、`show`、`bench` 五个子命令。

一次 live 的顺序：WS frame → 有界 WAL 队列 → 单写线程落盘（CRC32/seq/分段 SHA-256）→ 解析 → L2 订单簿 → 因子 → 做市报价 → 模拟成交 → JSONL/指标/日志。REST 快照作为 `kind=2` 事件写进 WAL，replay 走同一个 `runtime/pipeline.py`，不需要网络，也不使用墙钟或随机数。

## 1. 模块地图

| 路径 | 职责 |
|---|---|
| `cli.py`、`__main__.py` | 命令行入口；加载配置、覆盖参数、启动 live/replay/verify |
| `config.py` | 读取 TOML，映射到 dataclass，校验参数；处理环境变量 |
| `events.py` | 统一事件类型：深度、成交、bookTicker、快照、因子、信号、成交 |
| `normalize.py` | Binance payload → 内部事件；价格/数量转整数 tick；快照编解码 |
| `feed/binance.py` | 组合流 WebSocket、心跳、重连、快照请求触发 |
| `feed/rest.py` | Binance REST 深度快照、超时和重试 |
| `store/wal.py` | 二进制 WAL：`TFW1` 头、kind、长度、CRC32、recv 时间、全局 seq、分段轮转、fsync 策略 |
| `store/derived.py` | `factors.jsonl` / `signals.jsonl` / `fills.jsonl` 追加写和 SHA-256 |
| `book/orderbook.py` | L2 订单簿；Binance `U/u` 规则；gap 缓冲、交叉检测、快照恢复 |
| `factor/engine.py` | 每 symbol 的滚动因子：microprice、spread、OFI、top5 深度不平衡、成交不平衡、EWMA 波动、5s 动量 |
| `strategy/mm.py` | 确定性做市策略：公允价、库存偏移、半价差、双边/单边报价、冷却 |
| `exec/simulator.py` | 抽象 maker 撮合：报价被后续成交穿越时成交；手续费、平均成本、持仓/现金、名义上限 |
| `runtime/engine.py` | live 主循环：收包、WAL 队列、处理队列、背压、健康、关闭排空 |
| `runtime/pipeline.py` | live 和 replay 共用的同步处理链：解析 → 订单簿 → 因子 → 策略 → 执行 |
| `runtime/trade_quality.py` | 按 symbol 检查 `aggTrade.a` 重复、回退、gap、缺失 ID |
| `replay/runner.py` | 从 WAL 确定性回放，比较计数和 factors/signals/fills 的 SHA-256 |
| `metrics/` | 线程安全计数器/直方图，Prometheus 文本输出 |
| `server/http.py` | `/healthz`、`/readyz`、`/status`、`/metrics` |
| `logging_setup.py` | 结构化日志（文件 + 控制台） |

## 2. 建议阅读顺序

模块按运行时调用顺序排列如下：

1. `cli.py`：命令和参数怎么进入系统；
2. `runtime/engine.py`：live 的队列、WAL、处理、关闭；
3. `feed/binance.py` → `store/wal.py`：数据怎么收、怎么落盘；
4. `book/orderbook.py`：订单簿怎么同步和恢复；
5. `runtime/pipeline.py` → `factor/engine.py`：因子怎么算；
6. `strategy/mm.py` → `exec/simulator.py`：报价怎么变成成交；
7. `replay/runner.py`：同一份 WAL 怎么逐字节复现；
8. `metrics/`、`server/http.py`：可观测性。

## 3. 确定性约束

派生记录不用墙钟/随机数；seq 在 append 前分配；快照当普通事件处理；同一 WAL + `config.json` 必须得到同一组 SHA-256。完整契约见 `docs/接口设计.md`。

## 4. 常用命令

运行、校验和回放命令见 `README.md` 的“怎么跑”和 `docs/运行手册.md`，这里不重复。

接口和字段细节见 `docs/接口设计.md`，数据流与失败模式见 `docs/系统架构.md`，实测见 `docs/性能实测.md`。
