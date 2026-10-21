# Docker 构建与运行验证（P0）

题目要求打包 Docker image，保证代码能正常运行。本机的情况是：开发机装了 Docker Desktop，但 WSL2 虚拟化组件不完整，Docker 引擎无法启动，所以本机没有构建和运行。为了不把这个缺口留到交付，我在 GitHub Actions 的干净 Docker 环境里用当前 main 跑了一遍：build、容器版本、离线 sample replay、60 秒 live、verify 和 replay 全部通过（run 39533252435，commit beae9ee，image sha256:75014967…，60 秒运行 19,538 帧、627 fills、0 丢失、`clean_shutdown=true`），并生成了可直接 `docker load` 的镜像包。证据在 `artifacts/experiments/docker/ci/`。下面的命令是给本机或其他 Docker 主机复跑用的。

## 1. 环境要求

- Docker Engine 24+ 或 Docker Desktop（含 Compose v2）
- 能拉取 `python:3.14-slim`（拉不动就配置镜像加速）
- 构建时如果走国内 PyPI：
  ```bash
  export PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
  ```

## 2. 一键验证

有 Docker 的机器上，我一般直接跑这个脚本：

```bash
cd 一诺资产
bash scripts/docker_verify.sh
```

脚本会按顺序做这几件事：

1. `docker compose build`，保存构建日志；
2. 在容器内执行 `python -m tickforge --version`；
3. 用镜像内自带的 `samples/demo_run` 做离线 replay（不依赖交易所网络）；
4. 在容器内做 60 秒 live 采样，输出到挂载的 `artifacts/`；
5. 对容器生成的 run 执行 `verify` 和 `replay --verify`；
6. 保存 `docker version`、`docker compose version`、镜像 inspect 摘要。

## 3. 手动命令

不想用脚本，或者某一步要单独重跑，就按下面的命令来：

镜像已设置 `ENTRYPOINT ["python", "-m", "tickforge"]`，因此 `docker compose run --rm tickforge` 后面直接接 `--version`、`live`、`verify` 或 `replay`，无需再次填写 `python -m tickforge`。

```bash
# 构建
docker compose build 2>&1 | tee artifacts/experiments/docker_build.log

# 容器内版本
docker compose run --rm tickforge --version

# 镜像自带样本的离线 replay（不要用 --no-derived，否则 digest 对比会失败）
docker compose run --rm tickforge \
  replay --run /app/samples/demo_run --out /tmp/sample-replay --verify

# 60 秒真实行情（挂载 artifacts -> /data）
docker compose run --rm tickforge \
  live --config config/default.toml --duration 60 --out /data --http-port 0

# 找到容器写入的 run
RUN_ID=$(ls -td artifacts/runs/* | head -1 | xargs basename)

# WAL 校验
docker compose run --rm tickforge \
  verify --run "/data/runs/$RUN_ID"

# 确定性回放
docker compose run --rm tickforge \
  replay --run "/data/runs/$RUN_ID" --out "/data/runs/$RUN_ID-replay" --verify
```

## 4. 需要保存的证据

提交材料里至少要有这些：

- `artifacts/experiments/docker_build.log`：构建成功日志；
- `docker compose run --rm tickforge --version` 输出；
- 容器内 live 运行生成的 `artifacts/runs/<run_id>/summary.json`（`clean_shutdown=true`、`lost_raw_frames=0`）；
- 容器内 `verify` 输出（`ok=true`）；
- 容器内 `replay --verify` 输出（`verified=true`、三个摘要一致）；
- 可选：`docker image inspect tickforge:0.1.0` 的镜像 ID/大小。

## 5. 验收判定

下面六条全部满足，我才会认为 P0 关闭：

- [x] `docker compose build` 退出码 0；
- [x] 容器内 `python -m tickforge --version` 正常；
- [x] 容器内离线样本 replay 返回 `verified=true`；
- [x] 容器内 live 运行生成 `summary.json` 且 `clean_shutdown=true`；
- [x] 容器内 `verify` 返回 `ok=true`；
- [x] 容器内 `replay --verify` 返回 `verified=true`，三个 SHA-256 与 live 一致。

2026-10-21 的 GitHub Actions run 39533252435 已满足以上六条；本机仍受 WSL2 限制，下面保留本机/其他 Docker 主机的复跑步骤。

## 5.1 CI 证据

- Run: https://github.com/LDv237/YINO_asset/actions/runs/39533252435
- Commit: `beae9ee`
- Image: `sha256:7501496739bf6f46c436810c402c3851600bab4d254a37d56505d23152689b7b`
- Build: 成功，日志 `artifacts/experiments/docker/ci/docker_build.log`
- 容器版本: `tickforge 0.1.0`
- 离线 sample replay: `verified=true`，counts/digests 全部一致
- 60 秒 live: `run_id=20261021T022948Z-1b8c`，19,538 帧，19,548 条 WAL，2,752 信号，627 fills，`clean_shutdown=true`，`lost_raw_frames=0`
- 队列峰值（CI 60 秒进程内计数器值）/ durable lag: `wal_queue_max=62`、`pipeline_queue_max=16`；durable p50/p95 约 262 ms（`interval` 批次口径，不是逐条）
- `verify`: `ok=true`，`summary_clean_shutdown=true`，`truncated_tail_bytes=0`
- `replay --verify`: `verified=true`、`deterministic=true`，三个 SHA-256 与 live 一致
- 证据: `artifacts/experiments/docker/ci/`，GitHub Actions artifact `docker-verify-evidence`

## 5.2 镜像包交付

Release 里附了 `tickforge-0.1.0.tar.gz`（`docker save` 生成，linux/amd64，50,901,849 字节 / 48.54 MiB；`docker load` 后镜像约 141.5 MiB），SHA-256 为 `6f188ec6e427f9ab1987ab853563ffd4f644fb9e99080f806a3e222776b09b90`，同名 `.sha256` 文件也在 Release 里。评审不需要本机构建，直接：

```bash
curl -L -O https://github.com/LDv237/YINO_asset/releases/download/delivery-20261020/tickforge-0.1.0.tar.gz
docker load -i tickforge-0.1.0.tar.gz
docker run --rm tickforge:0.1.0 --version

# 60 秒实时行情（需要外网）
docker run --rm -p 9100:9100 -v "$PWD/artifacts:/data" tickforge:0.1.0 \
  live --config config/default.toml --duration 60 --out /data
```

镜像构建于 `beae9ee`；之后的提交只改文档和 host-side 的 `tools/trace_signal.py`，`tickforge/`、`config/`、`scripts/`、`Dockerfile` 和依赖都没有变化，运行时代码与当前 main 一致。镜像标签和 `docker-compose.yml` 里的 `tickforge:0.1.0` 一致；入口是 `python -m tickforge`，离线样本在 `/app/samples/demo_run`，没有交易所网络也能验证：

```bash
docker run --rm tickforge:0.1.0 \
  replay --run /app/samples/demo_run --out /tmp/sample-replay --verify
```

## 6. 已知构建注意事项

- 镜像使用 `python:3.14-slim`；Docker Hub 不可达时要配 registry mirror；
- 容器以 root 运行，保证 bind mount 的 `./artifacts` 可写；生产环境应该改非 root 加固定 UID；
- Compose 的 `./artifacts:/data` 会把容器输出持久化到宿主机；
- 容器内 live 运行要能访问 `wss://data-stream.binance.vision` 和 `https://data-api.binance.vision`；离线 replay 不需要外网。
