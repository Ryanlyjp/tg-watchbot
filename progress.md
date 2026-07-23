## 2026-07-23 - Task: Fix Turnstile verification internal server error

### What was done

- Changed consumed or absent Turnstile verification tokens to persist as SQL `NULL` instead of a shared empty string, allowing multiple users to complete verification while preserving uniqueness for active tokens.
- Added a regression test covering two users completing Turnstile verification consecutively.
- Documented the expected behavior, failure signature, and verification command.
- Deployed the fix to the active `pchk` systemd installation without changing its environment, Cloudflare Tunnel, listener, or database schema.

### Testing

- Confirmed the new regression test failed before the fix with `sqlite3.IntegrityError: UNIQUE constraint failed: relay_user_state.verify_token`.
- Confirmed the targeted regression test passes after the fix.
- Ran the complete test suite: 69 tests passed.
- On `pchk`, verified `/health` returns `ok`, two consecutive Turnstile users can be marked verified against an isolated SQLite database, and both cleared tokens are stored as `NULL`.
- Confirmed `tg-watchbot` and `cloudflared` are active and the panel still listens only on `127.0.0.1:8765`.

### Notes

- `app.py`: stores missing or consumed verification tokens as SQL `NULL`.
- `tests/test_monitor_message_cleanup.py`: adds consecutive Turnstile verification regression coverage.
- `docs/turnstile-verification.md`: documents behavior, diagnosis, and validation.
- `progress.md`: records this task, validation evidence, and rollback point.
- Remote rollback point: `/opt/tg-watchbot/backups/20260723-082327`. Restore with `install -o tg-watchbot -g tg-watchbot -m 0644 /opt/tg-watchbot/backups/20260723-082327/app.py /opt/tg-watchbot/app.py && systemctl restart tg-watchbot`.

## 2026-07-23 - Task: Migrate pchk tg-watchbot to hardened non-root Docker

### What was done

- Separated runtime data from image code so SQLite WAL, configuration, environment settings, logs, and Forwarder data remain persistent while the image root filesystem stays read-only.
- Built the image with a dedicated non-root UID/GID and deployed it with all capabilities dropped, `no-new-privileges`, a private `/tmp`, a PID limit, localhost-only port binding, and no Docker socket or privileged access.
- Preserved panel-triggered restart behavior through Docker's `unless-stopped` policy and kept Cloudflare Tunnel pointed at the unchanged host origin `127.0.0.1:8765`.
- Migrated the live SQLite database and runtime files to `/opt/tg-watchbot/docker-data`, disabled the previous systemd service, and retained it as the rollback path.

### Testing

- Ran the complete application test suite: 70 tests passed.
- Built and ran hardened candidate containers locally and on `pchk` without Bot tokens before cutover.
- Confirmed UID 999/GID 994, zero effective capabilities, `NoNewPrivs: 1`, read-only image root, and writable `/data` only.
- Confirmed the formal container is healthy, uses restart policy `unless-stopped`, and publishes port 8765 only on host `127.0.0.1`.
- Confirmed local and Cloudflare Tunnel health endpoints return HTTP 200, Bot polling started without conflict, and all 15 configured monitors loaded.
- Confirmed SQLite integrity is `ok`, journal mode is WAL, the Forwarder example remains available, and consecutive Turnstile state writes pass inside the formal container.
- Confirmed the panel restart endpoint increments Docker restart count and returns healthy using a no-token candidate container.

### Notes

- `app.py`: supports a separate runtime data directory and an internal container-only panel bind override.
- `Dockerfile`: creates the non-root image user, runtime data path, and image health check.
- `.dockerignore`: excludes environment files, runtime data, backups, and Forwarder secrets from the image context.
- `docker-compose.yml`: defines the hardened container boundary and localhost-only port mapping for Compose-capable hosts.
- `README.md`: updates Docker installation instructions for `docker-data` and matching host UID/GID.
- `tests/test_runtime_paths.py`: verifies runtime data can be moved outside the code directory.
- `docs/docker-hardening.md`: documents the security boundary, current `pchk` deployment, validation, and rollback commands.
- `progress.md`: records migration evidence and rollback details.
- The existing malformed XML response from the 恩山 RSS source is unchanged and unrelated to Docker migration.
- The migrated application log already occupies approximately 2.4 GB and remains an unbounded application log; log rotation should be handled as a separate scoped task.
- Remote rollback point: `/opt/tg-watchbot/backups/docker-migration-20260723-084053`. The executable rollback sequence is recorded in `docs/docker-hardening.md`.
