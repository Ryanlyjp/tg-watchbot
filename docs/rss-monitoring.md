# RSS Monitoring

## IDCFLARE and SB.SB

The forum templates include these feeds:

- IDCFLARE latest topics: `https://idcflare.com/latest.rss`
- SB.SB latest topics: `https://sb.sb/rss.xml`

Both templates use `baseline_on_first_run: true`, so adding them records the current feed without sending historical posts.

## Cloudflare handling

SB.SB is fetched with the ordinary RSS client. IDCFLARE returns a Cloudflare browser challenge to ordinary clients and to `curl`, so the application falls back to the internal `flaresolverr` Docker service only after detecting that challenge.

FlareSolverr is not published on a host port. It is available only to the `tg-watchbot` container on the Docker network. Chromium renders RSS as an HTML preview with the XML inside a `pre` element; the application extracts that XML before passing it to the existing RSS parser.

## Validation and rollback

After deployment, open the monitor preview or run a manual check for each source. A successful IDCFLARE check logs `Flaresolverr fallback` and should not log `monitor failed`.

To roll back the integration, restore the previous `app.py`, `docker-compose.yml`, `config.yaml`, and monitor database backup, then run `docker-compose up -d --build --force-recreate`. Do not delete `docker-data/tg-watchbot.sqlite3` unless its data has been backed up separately.
