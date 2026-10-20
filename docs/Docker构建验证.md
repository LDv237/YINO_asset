# Docker 构建与运行验证（P0）

题目要求打包 Docker image，保证代码能正常运行。我先把本机的情况说清楚：开发机装了 Docker Desktop，但 WSL2 虚拟化组件不完整，Docker 引擎无法启动，所以 Docker 还没有实际构建和运行，交付证据是不完整的。这一点不能含糊，Docker 不是可选项。本机排障记录在 `artifacts/experiments/docker/INSTALL_STATUS.md`。下面是我在有 Docker 的机器上会执行的命令，以及需要保存的证据。

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
  replay --run "/data/runs/$RUN_ID" --verify
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

- [ ] `docker compose build` 退出码 0；
- [ ] 容器内 `python -m tickforge --version` 正常；
- [ ] 容器内离线样本 replay 返回 `verified=true`；
- [ ] 容器内 live 运行生成 `summary.json` 且 `clean_shutdown=true`；
- [ ] 容器内 `verify` 返回 `ok=true`；
- [ ] 容器内 `replay --verify` 返回 `verified=true`，三个 SHA-256 与 live 一致。

在 Docker 验证完成之前，报告和交付说明里我都写的是“Docker 未实际构建验证”，没有写成已完成。

## 6. 已知构建注意事项

- 镜像使用 `python:3.14-slim`；Docker Hub 不可达时要配 registry mirror；
- 容器以 root 运行，保证 bind mount 的 `./artifacts` 可写；生产环境应该改非 root 加固定 UID；
- Compose 的 `./artifacts:/data` 会把容器输出持久化到宿主机；
- 容器内 live 运行要能访问 `wss://data-stream.binance.vision` 和 `https://data-api.binance.vision`；离线 replay 不需要外网。
