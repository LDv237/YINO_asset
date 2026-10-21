# Docker CI 证据（节选）

这里保留最终 main `4d634cd` 的 60 秒容器 live 关键证据：`run_info.txt`、`container_summary.json`、`container_replay_summary.json`、`verify.log`、`replay_verify.log`、`image_id.txt`、`tickforge-0.1.0.tar.gz.sha256`。

- CI run: 39598128432（workflow_dispatch，commit `4d634cd`）
- 镜像: `tickforge:0.1.0`，49,317,333 bytes，SHA-256 `ca3891c2e15c68f638fc5d4b375e78da7f0c851e79745f81e5057aeca41ab4be`
- 60 秒 live: 17,171 帧，17,179 条 WAL，2,574 信号，486 fills，0 丢失，`clean_shutdown=true`

完整的 build 日志、镜像 inspect、版本日志在 GitHub Release 的 `artifacts/experiments/docker/ci/`。
