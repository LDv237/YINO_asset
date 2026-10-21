# Docker CI 证据（节选）

本目录保留 main 分支 `3bfd6ed` 的 60 秒容器 live 关键证据：`run_info.txt`、`container_summary.json`、`container_replay_summary.json`、`verify.log`、`replay_verify.log`、`image_id.txt`、`tickforge-0.1.0.tar.gz.sha256`。

- CI run: 39654447001（workflow_dispatch，commit `3bfd6ed`）
- 镜像: `tickforge:0.1.0`，49,316,109 bytes，SHA-256 `a07d37e9dcb88805f991e57a646aca5c30651c39076695cdbd6acfa6d2552909`
- 60 秒 live: 18,286 帧，18,296 条 WAL，2,643 信号，404 fills，0 丢失，`clean_shutdown=true`
- `verify`: `ok=true`，`derived_complete=true`、`derived_digests_complete=true`
- 健康快照: `health_before_shutdown.ok=true`、`health_final.ok=true`、`oldest_pending_ms=0.0`

完整的 build 日志、镜像 inspect、版本日志在 run 39654447001 的 GitHub Actions artifact `docker-verify-evidence`；2026-10-20 的 artifacts 归档在 Release 的 `YINO_artifacts_complete_20261020.zip`。
