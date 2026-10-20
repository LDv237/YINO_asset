# Docker 构建与运行验证（P0）

题目要求打包 Docker image，保证代码能正常运行。本机的情况是：开发机装了 Docker Desktop，但 WSL2 虚拟化组件不完整，Docker 引擎无法启动，所以本机没有构建和运行。为了不把这个缺口留到交付，我在 GitHub Actions 的干净 Docker 环境里跑了一遍：build、容器版本、离线 sample replay、60 秒 live、verify 和 replay 全部通过（run 39457220374，commit 536b181，image sha256:e67c76fd6a54d1011248c59acdc3d1522e00659d5495beaeed4ba4356b46e949，60 秒运行 30,444 帧、0 丢失、`clean_shutdown=true`）。证据在 `artifacts/experiments/docker/ci/`。下面的命令是给本机或其他 Docker 主机复跑用的。

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

2026-10-20 的 GitHub Actions run 39457220374 已满足以上六条；本机仍受 WSL2 限制，下面保留本机/其他 Docker 主机的复跑步骤。

## 5.1 CI 证据

- Run: https://github.com/LDv237/YINO_asset/actions/runs/39457220374
- Commit: `536b181`
- Image: `sha256:e67c76fd6a54d1011248c59acdc3d1522e00659d5495beaeed4ba4356b46e949`
- Build: 成功，日志 `artifacts/experiments/docker/ci/docker_build.log`
- 容器版本: `tickforge 0.1.0`
- 离线 sample replay: `verified=true`，counts/digests 全部一致
- 60 秒 live: `run_id=20261020T162519Z-a42f`，30,444 帧，30,450 条 WAL，3,016 信号，711 fills，`clean_shutdown=true`，`lost_raw_frames=0`
- `verify`: `ok=true`，`summary_clean_shutdown=true`，`truncated_tail_bytes=0`
- `replay --verify`: `verified=true`、`deterministic=true`，三个 SHA-256 与 live 一致
- 证据: `artifacts/experiments/docker/ci/`，GitHub Actions artifact `docker-verify-evidence`

## 6. 已知构建注意事项

- 镜像使用 `python:3.14-slim`；Docker Hub 不可达时要配 registry mirror；
- 容器以 root 运行，保证 bind mount 的 `./artifacts` 可写；生产环境应该改非 root 加固定 UID；
- Compose 的 `./artifacts:/data` 会把容器输出持久化到宿主机；
- 容器内 live 运行要能访问 `wss://data-stream.binance.vision` 和 `https://data-api.binance.vision`；离线 replay 不需要外网。
