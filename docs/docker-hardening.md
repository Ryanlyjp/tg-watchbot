# Hardened Docker deployment

## Security boundary

The `tg-watchbot` container runs as a dedicated non-root UID/GID with:

- all Linux capabilities dropped;
- `no-new-privileges` enabled;
- a read-only container root filesystem;
- a private writable `/tmp` tmpfs;
- no Docker socket, privileged mode, host PID namespace, or host network;
- host port `8765` bound only to `127.0.0.1`.

Cloudflare Tunnel continues to use the host origin `http://127.0.0.1:8765`.

## Writable data

Application code remains inside the read-only image. Only these paths are writable:

```text
/data/.env
/data/config.yaml
/data/tg-watchbot.sqlite3
/data/tg-watchbot.sqlite3-wal
/data/tg-watchbot.sqlite3-shm
/data/tg-watchbot.log
/data/forwarder/
```

The host maps `docker-data/` to `/data` and maps the existing `forwarder/` directory to `/data/forwarder`.

The application reads `TG_WATCHBOT_DATA_DIR=/data` from the image. `WEB_PANEL_BIND_HOST=0.0.0.0` is an internal container bind override; the value stored in `.env` can remain `127.0.0.1` for systemd rollback.

## Preserved behavior

- Telegram polling, RSS monitoring, SQLite WAL, Turnstile, panel settings, and logs use the persistent data mount.
- Saving `.env` in the panel persists to the host. The application reloads it when the container restarts.
- The panel restart action exits PID 1; Docker's `unless-stopped` policy starts it again.
- The optional Forwarder configuration remains in the existing host `forwarder/` directory.

The panel's Git update/rollback page requires a writable Git working tree. It was not functional on the existing `/opt/tg-watchbot` systemd installation because that directory was not a Git repository, and it remains unavailable in the read-only image.

## Validation checklist

After deployment, verify:

```bash
docker inspect tg-watchbot --format '{{.Config.User}} {{.HostConfig.ReadonlyRootfs}} {{.HostConfig.CapDrop}} {{.HostConfig.SecurityOpt}}'
docker exec tg-watchbot sh -c 'grep -E "^(Uid|Gid|CapEff|NoNewPrivs):" /proc/1/status'
curl --fail http://127.0.0.1:8765/health
docker logs --tail 100 tg-watchbot
```

Expected results include a non-zero UID, `CapEff: 0000000000000000`, `NoNewPrivs: 1`, a read-only root filesystem, and health output `ok`.

## Current pchk deployment

`pchk` uses the Docker CLI because the Compose plugin is not installed. The active deployment is:

```text
image: tg-watchbot:local
container: tg-watchbot
runtime data: /opt/tg-watchbot/docker-data
forwarder data: /opt/tg-watchbot/forwarder
host listener: 127.0.0.1:8765
restart policy: unless-stopped
```

The previous `tg-watchbot.service` unit remains installed but is disabled and inactive for rollback.

## Rollback to systemd

Stop and remove the container, copy the current runtime data back to the systemd paths, ensure `.env` has `WEB_PANEL_HOST=127.0.0.1`, then enable and start `tg-watchbot.service`. Always use the deployment-specific backup path recorded in `progress.md`.

For the current `pchk` deployment:

```bash
docker update --restart=no tg-watchbot
docker stop -t 30 tg-watchbot
docker rm tg-watchbot
install -o tg-watchbot -g tg-watchbot -m 0600 /opt/tg-watchbot/docker-data/.env /opt/tg-watchbot/.env
install -o tg-watchbot -g tg-watchbot -m 0644 /opt/tg-watchbot/docker-data/config.yaml /opt/tg-watchbot/config.yaml
install -o tg-watchbot -g tg-watchbot -m 0600 /opt/tg-watchbot/docker-data/tg-watchbot.sqlite3 /opt/tg-watchbot/tg-watchbot.sqlite3
install -o tg-watchbot -g tg-watchbot -m 0600 /opt/tg-watchbot/docker-data/tg-watchbot.log /opt/tg-watchbot/tg-watchbot.log
systemctl enable --now tg-watchbot
```
