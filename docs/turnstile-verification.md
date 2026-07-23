# Turnstile verification

## Expected behavior

- Each pending user receives a unique, expiring Turnstile token.
- After Cloudflare accepts the challenge, the user is marked verified and the consumed token is cleared.
- A cleared token is stored as SQL `NULL`, allowing multiple verified users while retaining the unique constraint for active tokens.
- The verification page then sends the normal welcome message and reports success.

## Failure diagnosis

If the page returns HTTP 500 after Cloudflare `siteverify` returns HTTP 200, check the `tg-watchbot` service log. A message like the following indicates an old build that stored cleared tokens as an empty string:

```text
sqlite3.IntegrityError: UNIQUE constraint failed: relay_user_state.verify_token
```

This does not indicate an invalid Turnstile Site Key, Secret Key, or Cloudflare Tunnel configuration.

## Verification

Run the regression test that covers two users completing verification consecutively:

```bash
python -m unittest tests.test_monitor_message_cleanup.MonitorMessageCleanupTest.test_multiple_turnstile_users_can_be_marked_verified -v
```

No database schema migration is required. Existing active verification links remain valid; their token becomes `NULL` when verification succeeds.
