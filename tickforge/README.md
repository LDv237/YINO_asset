# tickforge 包说明

这是 TickForge 的核心运行时包。入口是 `python -m tickforge`（`__main__.py` → `cli.py`），支持 `live`、`replay`、`verify`、`show`、`bench` 五个子命令。

一次 live 的数据流：

```text
WebSocket frame
  → 原始字节进有界 WAL 队列
  → 单写线程按 seq 落盘（CRC32 + 分段 SHA-256）
  → 解析 / 标准化
  → L2 订单簿（U/u 连续性，丢包时 REST 快照 + 缓冲重放）
  → 因子快照（默认 100 ms）
  → 做市策略报价
  → 模拟 maker 成交
  → factors / signals / fills JSONL + 指标 + 日志
```

REST 快照作为 `kind=2` 事件写进 WAL 的同一顺序位置；replay 走完全相同的 `runtime/pipeline.py`，不需要网络，也不使用墙钟或随机数。

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

第一次看建议按这条链路读，不用从每个文件第一行开始：

1. `cli.py`：命令和参数怎么进入系统；
2. `runtime/engine.py`：live 的队列、WAL、处理、关闭；
3. `feed/binance.py` → `store/wal.py`：数据怎么收、怎么落盘；
4. `book/orderbook.py`：订单簿怎么同步和恢复；
5. `runtime/pipeline.py` → `factor/engine.py`：因子怎么算；
6. `strategy/mm.py` → `exec/simulator.py`：报价怎么变成成交；
7. `replay/runner.py`：同一份 WAL 怎么逐字节复现；
8. `metrics/`、`server/http.py`：可观测性。

## 3. 确定性约束

- 派生记录（factors/signals/fills）不使用 `time.time()`、随机数或墙钟做决策；接收时间只用于延迟统计，不写进派生记录；
- WAL 的 seq 在 append 前分配，seq 顺序、物理写顺序、处理顺序一致；
- 快照当普通输入事件处理，live 和 replay 的插入位置一致；
- 相同 WAL + 相同 `config.json` 必须得到相同的三个 SHA-256。

## 4. 常用命令

```bash
python -m tickforge live --config config/default.toml --duration 900 --out artifacts
python -m tickforge verify --run artifacts/runs/<run_id>
python -m tickforge replay --run artifacts/runs/<run_id> --out artifacts/runs/<run_id>-replay --verify
python -m tickforge show --run artifacts/runs/<run_id>
python -m tickforge bench --run artifacts/runs/<run_id> --repeat 3
```

接口和字段细节见 `docs/接口设计.md`，数据流与失败模式见 `docs/系统架构.md`，实测见 `docs/性能实测.md`。
