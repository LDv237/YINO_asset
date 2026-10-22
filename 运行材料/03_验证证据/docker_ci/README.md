# Docker CI 证据（节选）

本目录保留 commit `71af930` 的 60 秒容器 live 关键证据：`run_info.txt`、`container_summary.json`、`container_replay_summary.json`、`verify.log`、`replay_verify.log`、`image_id.txt`、`tickforge-0.1.0.tar.gz.sha256`。

- CI run: 39731008808（workflow_dispatch，commit `71af930`）
- 镜像: `tickforge:0.1.0`，49,330,888 bytes，SHA-256 `8fc46c270afb7340cf1ff4fd69c6a7d85614606484ccf818436da0b7abc7d449`
- 60 秒 live: 21,993 帧，22,002 条 WAL，2,872 信号，632 fills，0 丢失，`clean_shutdown=true`
- `verify`: `ok=true`，`derived_complete=true`、`derived_digests_complete=true`
- 健康快照: `health_before_shutdown.ok=true`、`health_final.ok=true`、`oldest_pending_ms=0.0`

完整的 build 日志、镜像 inspect、版本日志在 run 39731008808 的 GitHub Actions artifact `docker-verify-evidence`；2026-10-20 的 artifacts 归档在 Release 的 `YINO_artifacts_complete_20261020.zip`。
