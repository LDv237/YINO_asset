# config 目录说明

本目录存放 TickForge 的运行时配置。三个 `.toml` 文件均为 TOML 格式的纯文本配置，不含可执行内容；程序启动时由 `tickforge/config.py` 读取、映射到配置对象并校验。

## 1. 三个配置文件的用途

| 文件 | 用途 | 关键差异 | 对应 run |
|---|---|---|---|
| `default.toml` | 抽象做市，执行语义/性能对照 | `allow_short = true`、`initial_cash_usd = 0`、`fsync_policy = "interval"` | 3049 抽象对照 / 53b5 历史基准 |
| `spot_long_only.toml` | 现货执行语义 | `allow_short = false`、`initial_cash_usd = 10000.0`；feed/因子/策略其余相同 | **9821 主结果** / 同一 WAL 对照 |
| `spot_long_only_durable.toml` | 现货 + 严格落盘 | 在 `spot_long_only` 基础上把 `fsync_policy` 改成 `"always"` | 6db8 严格落盘（Release） |

运行命令见 [`README.md`](../README.md) 的“运行方式”；把 `--config` 换成对应文件即可。

## 2. 字段分组

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
| `allow_short` | `true` 允许抽象负仓位；`false` 为现货语义 |
| `initial_cash_usd` | 每个 symbol 的初始现金；现货版为 10000 |
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

## 3. 覆盖优先级

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

## 4. 注意事项

- 未知的 section 会被拒绝；未知的 key 会被忽略，方便旧配置兼容；
- `allow_short = false` 只改变模拟执行语义，不会连接真实交易所，也不会真实下单；
- `fsync_policy = "interval"` 不是掉电零丢失：正常关闭会排空并 fsync，崩溃时未同步尾部可能丢；严格口径用 `always`；
- 配置里没有 API Key、Cookie 或账户信息，只有公开行情地址和运行参数。

更完整的运行说明见 `docs/09_运行与复现手册.md`，持久化边界见 `docs/03_性能测试与实测结果.md`。
