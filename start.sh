#!/usr/bin/env sh
set -e

PORT="${PORT:-8080}"
FASTAPI_ROOT_PATH="${FASTAPI_ROOT_PATH:-/api}"

sed "s/\${PORT}/${PORT}/g" /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf

export FASTAPI_ROOT_PATH
export BACKEND_BASE_URL="${BACKEND_BASE_URL:-http://127.0.0.1:${PORT}${FASTAPI_ROOT_PATH}}"

exec supervisord -c /etc/supervisor/conf.d/supervisord.conf
