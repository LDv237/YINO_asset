# Docker CI 证据（节选）

这里保留最终 main `d85db40` 的 60 秒容器 live 关键证据：`run_info.txt`、`container_summary.json`、`container_replay_summary.json`、`verify.log`、`replay_verify.log`、`image_id.txt`、`tickforge-0.1.0.tar.gz.sha256`。

- CI run: 39617196987（workflow_dispatch，commit `d85db40`）
- 镜像: `tickforge:0.1.0`，49,315,893 bytes，SHA-256 `57ce97fed32a21cd9f39c849a9a8864016812bb627e9ad98df2b646339a85cb1`
- 60 秒 live: 62,617 帧，62,629 条 WAL，3,417 信号，1,359 fills，0 丢失，`clean_shutdown=true`
- `verify`: `ok=true`，`derived_complete=true`、`derived_digests_complete=true`

完整的 build 日志、镜像 inspect、版本日志在 run 39617196987 的 GitHub Actions artifact `docker-verify-evidence`；2026-10-20 的历史 artifacts 仍在 Release 的 `YINO_artifacts_complete_20261020.zip`。
