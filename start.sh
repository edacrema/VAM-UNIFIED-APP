#!/usr/bin/env sh
set -e

PORT="${PORT:-8080}"

exec python -m streamlit run Home.py \
    --server.port "${PORT}" \
    --server.address 0.0.0.0 \
    --server.headless true \
    --server.enableCORS false \
    --server.enableXsrfProtection false
