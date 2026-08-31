# RSS Monitoring

## IDCFLARE and SB.SB

The forum templates include these feeds:

- IDCFLARE latest topics: `https://idcflare.com/latest.rss`
- SB.SB latest topics: `https://sb.sb/rss.xml`

Both templates use `baseline_on_first_run: true`, so adding them records the current feed without sending historical posts.

## Linux.SB web monitoring

Linux.SB does not expose a public RSS or Atom feed. Its `linuxsb` forum template polls `https://linux.sb/index.php?sort=post` every 180 seconds and extracts the topic cards with `.post-list .post-item` and `.post-title`. This view sorts ordinary topics by publication time instead of latest reply time, so replies to an older topic do not move it back into the new-topic window.

It is configured as a forum monitor with `baseline_on_first_run: true`, so the current page items are remembered without sending historical notifications. Each `/topic/<id>` URL is used as the stable item key, preventing a title edit or pinned topic from being treated as a new post. The application also identifies the `linux.sb` host as a forum monitor when a panel edit has omitted its `forum` field, and matches legacy title-keyed state by topic URL during the transition. Changing from the default homepage to the publication-time view keeps the same monitor name and topic keys, so existing deduplication state remains valid.

## Telegram notification content

Forum notifications sent to Telegram include the title, link, and match reason. Telegram already displays the delivery time, so published and check times are omitted from the message only; monitor events and their creation time remain available in the panel. Linux.SB also omits author and category because its homepage topic list does not provide them.

## Cloudflare handling

SB.SB is fetched with the ordinary RSS client. IDCFLARE returns a Cloudflare browser challenge to ordinary clients and to `curl`, so the application falls back to the internal `flaresolverr` Docker service only after detecting that challenge.

FlareSolverr is not published on a host port. It is available only to the `tg-watchbot` container on the Docker network. Chromium renders RSS as an HTML preview with the XML inside a `pre` element; the application extracts that XML before passing it to the existing RSS parser.

## DNS on pchk

The `pchk` host uses `systemd-resolved` with DNS over TLS. DNSSEC validation is disabled in `/etc/systemd/resolved.conf` because several valid RSS hosts, including `linux.sb`, return an incomplete DNSSEC chain and otherwise fail to resolve from both the host and Docker containers. The configured upstream resolvers remain Cloudflare and Google.

After changing that file, run `systemctl restart systemd-resolved` and verify both paths:

```bash
getent ahostsv4 linux.sb
docker exec tg-watchbot getent ahostsv4 linux.sb
```

To roll back, restore the timestamped `/etc/systemd/resolved.conf.bak-*` backup created before the change, then restart `systemd-resolved`.

## Validation and rollback

After deployment, open the monitor preview or run a manual check for each source. A successful IDCFLARE check logs `Flaresolverr fallback` and should not log `monitor failed`.

To roll back the integration, restore the previous `app.py`, `docker-compose.yml`, `config.yaml`, and monitor database backup, then run `docker-compose up -d --build --force-recreate`. Do not delete `docker-data/tg-watchbot.sqlite3` unless its data has been backed up separately.
