#!/usr/bin/env bash
# Install or update polyweather on an Ubuntu host. Run as root from a checkout:
#   sudo ./deploy/install.sh
# Safe to run again: it updates the code, keeps /etc/polyweather.env and the data.
set -euo pipefail

SRC="$(cd "$(dirname "$0")/.." && pwd)"
APP=/opt/polyweather
DATA=/var/lib/polyweather

if [[ $EUID -ne 0 ]]; then echo "run as root (sudo)"; exit 1; fi

apt-get update -qq
apt-get install -y -qq python3 python3-venv rsync curl

id polyweather >/dev/null 2>&1 || useradd --system --home-dir "$APP" --shell /usr/sbin/nologin polyweather
mkdir -p "$APP" "$DATA"
rsync -a --delete --exclude '.git' --exclude '.venv' --exclude 'data' "$SRC"/ "$APP"/
chown -R polyweather:polyweather "$APP" "$DATA"

sudo -u polyweather python3 -m venv "$APP/.venv"
sudo -u polyweather "$APP/.venv/bin/pip" install --quiet --upgrade pip
sudo -u polyweather "$APP/.venv/bin/pip" install --quiet -e "$APP"

[[ -f /etc/polyweather.env ]] || install -m 640 -g polyweather "$APP/deploy/polyweather.env.example" /etc/polyweather.env

install -m 644 "$APP"/deploy/systemd/*.service "$APP"/deploy/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload

run() { sudo -u polyweather env $(grep -v '^#' /etc/polyweather.env | xargs) "$APP/.venv/bin/polyweather" "$@"; }

# First catalog scan in import mode, so rule changes that already happened freeze nothing.
run catalog scan --history --closed-days 3
# Recent METAR history, so the first settlement checks see complete days.
run metar backfill-awc --days 4

# restart, not just start: after an update the two long-running services must pick up
# the new code (the recorder writes out what it holds and logs the pause as a gap)
systemctl enable polyweather-recorder.service polyweather-metar.service
systemctl restart polyweather-recorder.service polyweather-metar.service
for t in catalog nbm lamp ensemble prices check-settlement check-gaps paper paper-prepare paper-report; do
  systemctl enable --now "polyweather-$t.timer"
done

# History the paper nowcast needs (a month of reports and LAMP cycles): fetch it now, in the background.
systemctl start --no-block polyweather-paper-prepare.service

echo
echo "Installed. Useful commands:"
echo "  systemctl list-timers 'polyweather-*'"
echo "  journalctl -u polyweather-recorder -f"
echo "  sudo -u polyweather env \$(grep -v '^#' /etc/polyweather.env | xargs) $APP/.venv/bin/polyweather status"
