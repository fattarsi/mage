#!/usr/bin/env bash
# Start the XMage web gateway locally (no Docker). Reads optional config from a
# git-ignored .env (e.g. XMAGE_WEB_DECK_SOURCE_URL) so personal settings never
# get committed. Needs JDK 17+ and Maven — if not on PATH, it auto-loads the local
# toolchain at ~/.local/xmage-toolchain (or $XMAGE_TOOLCHAIN_ENV).
#
#   ./run-web.sh            # then open http://localhost:8080/
#   PORT=9000 ./run-web.sh  # custom port
# No JDK/Maven at all? Use Docker:  docker compose up --build
set -euo pipefail
cd "$(dirname "$0")"

# If Maven/JDK aren't on PATH, try to load a local toolchain (this repo ships one at
# ~/.local/xmage-toolchain). Override with XMAGE_TOOLCHAIN_ENV=/path/to/env.sh.
if ! command -v mvn >/dev/null 2>&1; then
  for env in "${XMAGE_TOOLCHAIN_ENV:-}" "$HOME/.local/xmage-toolchain/env.sh"; do
    if [ -n "$env" ] && [ -f "$env" ]; then
      # shellcheck disable=SC1090
      . "$env"
      break
    fi
  done
fi
if ! command -v mvn >/dev/null 2>&1; then
  echo "ERROR: 'mvn' (Maven) not found on PATH." >&2
  echo "Install JDK 17+ and Maven, or set XMAGE_TOOLCHAIN_ENV to a script that puts them on PATH," >&2
  echo "or run it in Docker instead:  docker compose up --build" >&2
  exit 1
fi

# Load .env if present (export each non-comment KEY=VALUE line).
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

PORT="${PORT:-8080}"
echo "Starting XMage web gateway on http://localhost:${PORT}/"
if [ -n "${XMAGE_WEB_DECK_SOURCE_URL:-}" ]; then
  echo "Deck source: ${XMAGE_WEB_DECK_SOURCE_URL}"
fi

# XMAGE_WEB_DECK_SOURCE_URL is read from the environment by WebServerMain.
exec mvn -o -pl Mage.Server.Web exec:java \
  -Dexec.mainClass=mage.server.web.WebServerMain \
  -Dxmage.web.port="${PORT}" \
  -Dxmage.config.path=Mage.Server/config/config.xml
