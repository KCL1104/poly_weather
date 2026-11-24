#!/usr/bin/env bash
# Run the whole system without Docker (macOS, Linux). Sets itself up on first use.
#
#   scripts/start.sh                 run until Ctrl+C
#   caffeinate -s scripts/start.sh   macOS: also keep the computer awake
#
# Settings: an optional .env file in the repository root (see .env.example).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

if [ ! -x .venv/bin/polyweather ]; then
  PY="${PYTHON:-python3}"
  if ! "$PY" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
    echo "Python 3.11 or newer is needed (set PYTHON=/path/to/python to choose one)." >&2
    exit 1
  fi
  "$PY" -m venv .venv
  .venv/bin/python -m pip install --quiet --disable-pip-version-check -e .
fi

export POLYWEATHER_DATA_DIR="${POLYWEATHER_DATA_DIR:-$PWD/data}"
exec .venv/bin/polyweather run-all "$@"
