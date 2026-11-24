#!/usr/bin/env bash
# Called by systemd when a polyweather unit fails. Posts a short message to the
# webhook in ALERT_WEBHOOK_URL (Slack- and Discord-compatible body); without one it
# only writes to the journal.
set -euo pipefail
unit="${1:-unknown}"
host="$(hostname)"
last="$(journalctl -u "$unit" -n 5 --no-pager -o cat 2>/dev/null | tail -c 900 || true)"
msg="polyweather on ${host}: ${unit} failed. ${last}"
echo "$msg"
if [[ -n "${ALERT_WEBHOOK_URL:-}" ]]; then
  body="$(python3 -c 'import json,sys; m=sys.argv[1]; print(json.dumps({"text": m, "content": m[:1900]}))' "$msg")"
  curl -fsS -m 20 -H 'Content-Type: application/json' --data "$body" "$ALERT_WEBHOOK_URL" >/dev/null || echo "alert webhook failed"
fi
