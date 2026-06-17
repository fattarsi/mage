#!/usr/bin/env bash
# Start the XMage web gateway locally (no Docker). Reads optional config from a
# git-ignored .env (e.g. XMAGE_WEB_DECK_SOURCE_URL) so personal settings never
# get committed. Requires JDK 17+ and Maven on PATH.
#
#   ./run-web.sh            # then open http://localhost:8080/
#   PORT=9000 ./run-web.sh  # custom port
set -euo pipefail
cd "$(dirname "$0")"

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
