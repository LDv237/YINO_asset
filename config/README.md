# config 目录说明

这个目录放 TickForge 的运行时配置。三个 `.toml` 文件不是代码，是 TOML 格式的纯文本配置；程序启动时由 `tickforge/config.py` 读取、映射到配置对象并校验，不执行任何配置内容。

## 1. TOML 是什么

TOML（Tom's Obvious, Minimal Language）是一种人类可读的配置格式：

```toml
[wal]
fsync_policy = "interval"   # 字符串
queue_max = 500000         # 整数
enabled = true             # 布尔

[feed]
symbols = ["BTCUSDT", "ETHUSDT"]
```

- `[section]` 是分组；
- `key = value` 是配置项；
- 支持字符串、整数、浮点、布尔、数组和 `#` 注释；
- 不依赖缩进，不像 YAML 那样容易踩缩进问题；
- Python 3.11+ 用标准库 `tomllib` 读取，不需要额外依赖。

`pyproject.toml` 也是 TOML，但那个是 Python 项目元数据；本目录的 `.toml` 是运行参数。

## 2. 三个配置文件怎么选

| 文件 | 用途 | 关键差异 | 对应 run |
|---|---|---|---|
| `default.toml` | 默认抽象做市，性能/链路基准 | `allow_short = true`、`initial_cash_usd = 0`、`fsync_policy = "interval"` | **3049 主结果** / 53b5 历史基准 |
| `spot_long_only.toml` | 现货只做多执行语义 | `allow_short = false`、`initial_cash_usd = 10000.0`；feed/因子/策略其余相同 | 同一 WAL 对照 |
| `spot_long_only_durable.toml` | 现货只做多 + 严格落盘 | 在 `spot_long_only` 基础上把 `fsync_policy` 改成 `"always"` | 6db8 严格落盘（Release） |

```bash
# 默认抽象做市
python -m tickforge live --config config/default.toml --duration 900 --out artifacts

# 现货只做多
python -m tickforge live --config config/spot_long_only.toml --duration 900 --out artifacts

# 现货只做多 + 每条记录 fsync
python -m tickforge live --config config/spot_long_only_durable.toml --duration 900 --out artifacts
```

## 3. 字段分组

### `[feed]`：数据源与连接

| 字段 | 含义 |
|---|---|
| `ws_url` / `rest_url` | Binance 公开行情 WebSocket / REST 地址；不需要 API Key |
| `symbols` | 订阅的交易对列表；默认 6 个 USDT 对 |
| `depth_speed_ms` | 深度流速度，`100` 或 `1000` 毫秒 |
| `stale_timeout_ms` | 多久没收到行情判定为 stale |
| `open_timeout_s` / `close_timeout_s` | 建连/关闭超时 |
| `ping_interval_s` / `ping_timeout_s` | WebSocket 心跳 |
| `reconnect_initial_s` / `reconnect_max_s` | 重连退避起始值和上限 |
| `snapshot_limit` / `snapshot_timeout_s` / `snapshot_retries` | REST 快照深度、超时和重试 |
| `message_max_bytes` | 单帧最大字节数，防止异常大包 |

### `[wal]`：原始数据落盘

| 字段 | 含义 |
|---|---|
| `segment_bytes` | WAL 分段大小，默认 64 MiB |
| `fsync_policy` | `interval`：每 250 ms 尝试组提交；`always`：每条记录 fsync 后才处理；`never`：只在关闭/轮转时同步 |
| `fsync_interval_ms` | `interval` 模式的目标间隔（不是硬上界） |
| `queue_max` | WAL 写队列上限，满了反压上游 |
| `record_max_bytes` | 单条 WAL payload 上限 |

### `[engine]`：处理流水线

| 字段 | 含义 |
|---|---|
| `proc_queue_max` | 处理队列上限 |
| `book_buffer_max` | 订单簿等待快照时的最大缓冲 |
| `factor_interval_ms` | 因子快照节流间隔，默认 100 ms |
| `stats_interval_s` / `health_interval_s` | 运行统计/健康检查周期 |
| `drain_timeout_s` | 关闭时排空队列的最长等待 |
| `max_processing_lag_ms` | 处理滞后告警阈值 |

### `[strategy]`：做市与执行

| 字段 | 含义 |
|---|---|
| `quote_notional_usd` | 每边报价名义金额 |
| `max_position_notional_usd` | 单 symbol 最大持仓名义 |
| `inventory_skew` | 库存偏移系数 |
| `ofi_coeff_bps` / `momentum_coeff` | OFI、动量对公允价的修正 |
| `min_half_spread_bps` / `max_half_spread_bps` / `vol_half_spread_coeff` | 半价差上下限与波动加成 |
| `fee_bps` | 模拟手续费 |
| `quote_cooldown_ms` | 同 symbol 两次报价的最小间隔 |
| `enabled` | 是否启用策略 |
| `allow_short` | `true` 允许抽象负仓位；`false` 为现货只做多语义 |
| `initial_cash_usd` | 每个 symbol 的初始现金；现货只做多版为 10000 |
| `round_to_tick` | bid 向下、ask 向上取到交易所 tick |

### `[http]`：观测接口

| 字段 | 含义 |
|---|---|
| `host` / `port` | `/healthz`、`/readyz`、`/status`、`/metrics` 监听地址 |
| `enabled` | 是否启动 HTTP 服务 |

### `[run]`：运行窗口

| 字段 | 含义 |
|---|---|
| `duration_s` | 采集窗口，15 分钟 run 为 900 秒 |
| `output_dir` | 运行产物根目录，默认 `artifacts` |
| `run_id` | 留空则自动生成时间戳+随机后缀 |
| `log_level` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `seed` | 预留的确定性种子；当前流程不使用随机数 |

## 4. 覆盖优先级

同一项配置按以下顺序生效，后面的覆盖前面的：

```text
config/*.toml  <  TICKFORGE_* 环境变量  <  命令行参数
```

常用的环境变量：`TICKFORGE_WS_URL`、`TICKFORGE_REST_URL`、`TICKFORGE_SYMBOLS`、`TICKFORGE_HTTP_HOST`、`TICKFORGE_HTTP_PORT`、`TICKFORGE_OUTPUT_DIR`、`TICKFORGE_DURATION_S`、`TICKFORGE_FSYNC_POLICY`、`TICKFORGE_LOG_LEVEL`。

例如：

```bash
TICKFORGE_FSYNC_POLICY=always \
python -m tickforge live --config config/default.toml --duration 60
```

命令行也能覆盖：

```bash
python -m tickforge live --config config/default.toml \
  --symbols BTCUSDT --duration 60 --fsync-policy always --out artifacts
```

## 5. 注意事项

- 未知的 section 会被拒绝；未知的 key 会被忽略，方便旧配置兼容；
- `allow_short = false` 只改变模拟执行语义，不会连接真实交易所，也不会真实下单；
- `fsync_policy = "interval"` 不是掉电零丢失：正常关闭会排空并 fsync，崩溃时未同步尾部可能丢；严格口径用 `always`；
- 配置里没有 API Key、Cookie 或账户信息，只有公开行情地址和运行参数。

更完整的运行说明见 `docs/运行手册.md`，持久化边界见 `docs/性能实测.md`。
