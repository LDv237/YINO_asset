# Docker 构建与运行验证（P0）

题目要求打包 Docker image，保证代码能正常运行。本机的情况是：开发机装了 Docker Desktop，但 WSL2 虚拟化组件不完整，Docker 引擎无法启动，所以本机没有构建和运行。为了不把这个缺口留到交付，我在 GitHub Actions 的干净 Docker 环境里用**最终 main** 跑了一遍：build、容器版本、离线 sample replay、60 秒 live、verify 和 replay 全部通过（run 39617196987，commit `d85db40`，image `sha256:c288b87e…`，60 秒运行 62,617 帧、1,359 fills、0 丢失、`clean_shutdown=true`），并生成了可直接 `docker load` 的镜像包。邮件包内的证据在 `运行材料/验证证据/docker_ci/`；完整 Actions 日志在 run 39617196987 的 `docker-verify-evidence` artifact，2026-10-20 的历史 artifacts 仍在 Release 的 `YINO_artifacts_complete_20261020.zip` 内。下面的命令是给本机或其他 Docker 主机复跑用的。

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
cd YINO_asset   # git clone 得到的目录；如果是解压邮件包，cd 到包根目录
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

一键脚本 [`scripts/docker_verify.sh`](../scripts/docker_verify.sh) 已覆盖 build、容器版本、离线 sample replay、60 秒 live、verify、replay 和证据收集。手动复跑时按脚本里的同名步骤执行即可；镜像入口是 `python -m tickforge`，`docker compose run --rm tickforge` 后面直接接 `--version` / `live` / `verify` / `replay`。

## 4. 验收判定

六项验收（build、容器版本、sample replay、60 秒 live、verify、replay）在 2026-10-21 的 GitHub Actions run 39617196987 上全部通过；结果和证据见 §5.1。本机仍受 WSL2 限制，其他 Docker 主机可按 `scripts/docker_verify.sh` 复跑。

## 5.1 CI 证据

- Run: https://github.com/LDv237/YINO_asset/actions/runs/39617196987
- Commit: `d85db40`
- Image: `sha256:c288b87ed5dd2deacd105932b7bbd2acd4ee5b94aa055e5e9ae9c77be322fda7`
- Build: 成功；完整日志在 run 39617196987 的 `docker-verify-evidence` artifact，邮件包 `docker_ci/` 保留 container/verify/replay 证据
- 容器版本: `tickforge 0.1.0`
- 离线 sample replay: `verified=true`，counts/digests 全部一致
- 60 秒 live: `run_id=20261021T143009Z-5788`，62,617 帧，62,629 条 WAL，3,417 信号，1,359 fills，`clean_shutdown=true`，`lost_raw_frames=0`
- 队列峰值（CI 60 秒进程内计数器值）/ durable lag: `wal_queue_max=105`、`pipeline_queue_max=16`；durable p50/p95 262/262 ms（`interval` 批次口径，不是逐条）
- `verify`: `ok=true`，`summary_clean_shutdown=true`、`derived_complete=true`、`derived_digests_complete=true`，`truncated_tail_bytes=0`
- `replay --verify`: `verified=true`、`deterministic=true`，三个 SHA-256 与 live 一致
- 证据: 邮件包内 `运行材料/验证证据/docker_ci/`；完整 Actions artifact 为 `docker-verify-evidence`（run 39617196987）；2026-10-20 的历史 artifacts 在 Release 的 `YINO_artifacts_complete_20261020.zip` 内

## 5.2 镜像包交付

Release 里附了 `tickforge-0.1.0.tar.gz`（`docker save` 生成，linux/amd64，49,315,893 字节 / 47.03 MiB；`docker load` 后镜像约 136.4 MiB），SHA-256 为 `57ce97fed32a21cd9f39c849a9a8864016812bb627e9ad98df2b646339a85cb1`，同名 `.sha256` 文件也在 Release 里。评审不需要本机构建，直接：

```bash
curl -L -O https://github.com/LDv237/YINO_asset/releases/download/delivery-20261020/tickforge-0.1.0.tar.gz
docker load -i tickforge-0.1.0.tar.gz
docker run --rm tickforge:0.1.0 --version

# 60 秒实时行情（需要外网）
docker run --rm -p 9100:9100 -v "$PWD/artifacts:/data" tickforge:0.1.0 \
  live --config config/default.toml --duration 60 --out /data
```

镜像由最终 main `d85db40` 构建，已包含成交时间 `T` 修正和派生 sink 关闭失败的 fail-closed 处理。镜像标签和 `docker-compose.yml` 里的 `tickforge:0.1.0` 一致；入口是 `python -m tickforge`，离线样本在 `/app/samples/demo_run`，没有交易所网络也能验证：

```bash
docker run --rm tickforge:0.1.0 \
  replay --run /app/samples/demo_run --out /tmp/sample-replay --verify
```

## 6. 已知构建注意事项

- 镜像使用 `python:3.14-slim`；Docker Hub 不可达时要配 registry mirror；
- 容器以 root 运行，保证 bind mount 的 `./artifacts` 可写；生产环境应该改非 root 加固定 UID；
- Compose 的 `./artifacts:/data` 会把容器输出持久化到宿主机；
- 容器内 live 运行要能访问 `wss://data-stream.binance.vision` 和 `https://data-api.binance.vision`；离线 replay 不需要外网。
