# scripts 目录说明

这里放可重复执行的入口脚本。它们只是把 `python -m tickforge ...` 和 `docker compose ...` 包了一层，方便复跑；不改变程序行为。所有脚本都默认从仓库根目录执行，依赖安装见 `docs/依赖说明.md`。

## 1. 脚本列表

| 脚本 | 平台 | 作用 |
|---|---|---|
| `run_live_15m.sh` | Git Bash / Linux / macOS | 跑 900 秒 live，默认 `config/default.toml`，输出到 `artifacts`；额外参数会透传 |
| `run_live_15m.ps1` | Windows PowerShell | 同上，PowerShell 版本 |
| `run_live_15m_docker.sh` | 有 Docker 的主机 | `docker compose build` + `up --abort-on-container-exit`，在容器里跑 15 分钟 |
| `docker_verify.sh` | 有 Docker 的主机 | 构建镜像并产出 Docker 证据：版本、离线 sample replay、60 秒 live、容器内 verify/replay |
| `replay_verify.sh` | Git Bash / Linux | 对一个 run 目录执行 `verify` + `replay --verify` |
| `extract_main_run.py` | 跨平台 Python | 解压主结果的 `run_data.tar.xz`（raw/ + derived/） |
| `run_fsync_experiments.sh` | Git Bash / Linux | 30 秒 BTCUSDT 的 `interval` / `always` / `never` 对照，输出到 `artifacts/experiments/fsync/` |

## 2. 常用命令

```bash
# 15 分钟 live（bash）
bash scripts/run_live_15m.sh

# 15 分钟 live（Windows PowerShell）
powershell -ExecutionPolicy Bypass -File scripts/run_live_15m.ps1

# 对主结果做校验和确定性回放
bash scripts/replay_verify.sh 运行材料/01_主结果_现货_9821

# fsync 对照（30 秒实验，需要交易所网络）
bash scripts/run_fsync_experiments.sh
```

## 3. 注意事项

- `run_live_15m*` 需要能访问 Binance 公开行情，不需要 API Key；
- `docker_verify.sh` / `run_live_15m_docker.sh` 需要 Docker 引擎；本机 WSL2 组件不完整，Docker 证据是在 GitHub Actions 的干净环境跑的（见 `docs/Docker构建验证.md`）；
- `replay_verify.sh` 的 `--out` 是 `<run_dir>-replay`，目录已存在时会由 replay 拒绝覆盖，先换输出目录或手动清理；
- 所有脚本只用于本地复现和验证，不会真实下单。

手动命令和完整说明见 `docs/运行手册.md`。
