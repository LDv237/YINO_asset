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
| `run_fsync_experiments.sh` | Git Bash / Linux | 30 秒 BTCUSDT 的 `interval` / `always` / `never` 对照，输出到 `artifacts/experiments/fsync/` |
| `reproduce_from_delivery.py` | 跨平台 Python | 从 `运行材料/02_正式运行/` 或 `运行材料/07_最终代码单源复跑/` 重建完整 run 目录，并执行 verify/replay |

## 2. 常用命令

```bash
# 15 分钟 live（bash）
bash scripts/run_live_15m.sh

# 15 分钟 live（Windows PowerShell）
powershell -ExecutionPolicy Bypass -File scripts/run_live_15m.ps1

# 对一个 run 做校验和确定性回放
bash scripts/replay_verify.sh artifacts/runs/20261020T072820Z-53b5

# 从分类附件重建并校验（默认 workdir 由 summary.run_id 决定）
python scripts/reproduce_from_delivery.py
python scripts/reproduce_from_delivery.py --delivery 运行材料/07_最终代码单源复跑

# 已有 workdir/replay 输出时，显式覆盖
python scripts/reproduce_from_delivery.py --force

# fsync 对照
bash scripts/run_fsync_experiments.sh
```

## 3. 注意事项

- `run_live_15m*` 需要能访问 Binance 公开行情，不需要 API Key；
- `docker_verify.sh` / `run_live_15m_docker.sh` 需要 Docker 引擎；本机 WSL2 组件不完整，Docker 证据是在 GitHub Actions 的干净环境跑的（见 `docs/Docker构建验证.md`）；
- `replay_verify.sh` 的 `--out` 是 `<run_dir>-replay`，目录已存在时会由 replay 拒绝覆盖，先换输出目录或手动清理；
- `reproduce_from_delivery.py` 默认拒绝覆盖非空 workdir/replay 输出，`--force` 才会替换；
- 所有脚本只用于本地复现和验证，不会真实下单。

手动命令和完整说明见 `docs/运行手册.md`。
