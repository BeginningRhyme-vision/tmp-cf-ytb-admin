#!/bin/bash
set -e

# setup_and_run.sh
# This script builds Docker images for the backend and frontend,
# and starts them as containers.

# Get the absolute path of the project root
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_FILE="$ROOT_DIR/config.yaml"

# --- Pre-flight Checks ---

echo ">>> Checking environment..."

if ! command -v docker &> /dev/null; then
    echo "Error: 'docker' is not installed."
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: config.yaml not found in project root ($CONFIG_FILE)."
    echo "Please create it before running this script."
    exit 1
fi

# --- Build Images ---

echo ">>> Building Backend Docker Image..."
# Context is backend/go-app as per Dockerfile comments
docker build -t unbound-backend "$ROOT_DIR/backend/go-app"

echo ">>> Building Frontend Docker Image..."
# Context is frontend
docker build -t unbound-frontend "$ROOT_DIR/frontend"

echo ">>> Building Worker Downloader Docker Image..."
docker build -t unbound-worker-downloader "$ROOT_DIR/backend/worker_downloader"

echo ">>> Building Worker Transfer Docker Image..."
docker build -t unbound-worker-transfer "$ROOT_DIR/backend/worker_transfer"

echo ">>> Building Worker FFmpeg Docker Image..."
docker build -t unbound-worker-ffmpeg "$ROOT_DIR/backend/worker_ffmpeg"

# --- Run Containers ---

echo ">>> Starting Backend Container..."
# Mount config.yaml to /app/config.yaml
# Run in detached mode first to get ID
BACKEND_ID=$(docker run -d \
    --network host \
    -v "$CONFIG_FILE:/app/config.yaml" \
    --name unbound-backend-instance \
    unbound-backend)

echo "  Backend started with ID: ${BACKEND_ID:0:12}"

echo ">>> Starting Frontend Container..."
# Frontend runs on host network (listens on port 80 inside container, so port 80 on host)
FRONTEND_ID=$(docker run -d \
    --network host \
    --name unbound-frontend-instance \
    unbound-frontend)

echo "  Frontend started with ID: ${FRONTEND_ID:0:12}"

# echo ">>> Starting Worker Downloader Container..."
# WORKER_DOWNLOADER_ID=$(docker run -d --rm \
#     --network host \
#     -e BACKEND_API_URL="http://localhost:8080/api" \
#     -v "$CONFIG_FILE:/app/config.yaml" \
#     --name unbound-worker-downloader-instance \
#     --entrypoint "downloader" \
#     unbound-worker-downloader)

# echo "  Worker Downloader started with ID: ${WORKER_DOWNLOADER_ID:0:12}"

# echo ">>> Starting Worker Metadata Container..."
# WORKER_METADATA_ID=$(docker run -d --rm \
#     --network host \
#     -e BACKEND_API_URL="http://localhost:8080/api" \
#     -v "$CONFIG_FILE:/app/config.yaml" \
#     --name unbound-worker-metadata-instance \
#     unbound-worker-downloader)

# echo "  Worker Metadata started with ID: ${WORKER_METADATA_ID:0:12}"

# echo ">>> Starting Worker Transfer Container..."
# WORKER_TRANSFER_ID=$(docker run -d --rm \
#     --network host \
#     -e BACKEND_API_URL="http://localhost:8080/api" \
#     -v "$CONFIG_FILE:/app/config.yaml" \
#     --name unbound-worker-transfer-instance \
#     --entrypoint "transfer" \
#     unbound-worker-transfer)

# echo "  Worker Transfer started with ID: ${WORKER_TRANSFER_ID:0:12}"

# echo ">>> Starting Worker Scanner Container..."
# WORKER_SCANNER_ID=$(docker run -d --rm \
#     --network host \
#     -e BACKEND_API_URL="http://localhost:8080/api" \
#     -v "$CONFIG_FILE:/app/config.yaml" \
#     --name unbound-worker-scanner-instance \
#     --entrypoint "scanner" \
#     unbound-worker-transfer)

# echo "  Worker Scanner started with ID: ${WORKER_SCANNER_ID:0:12}"

echo ">>> Starting Worker FFmpeg Container..."
WORKER_FFMPEG_ID=$(docker run -d --rm \
    --network host \
    -e BACKEND_API_URL="http://localhost:8080/api" \
    --name unbound-worker-ffmpeg-instance \
    unbound-worker-ffmpeg)

echo "  Worker FFmpeg started with ID: ${WORKER_FFMPEG_ID:0:12}"

# --- Cleanup Trap ---

cleanup() {
    echo ""
    echo ">>> Stopping containers..."
    docker stop "$BACKEND_ID" >/dev/null 2>&1 || true
    docker stop "$FRONTEND_ID" >/dev/null 2>&1 || true
    docker stop "$WORKER_DOWNLOADER_ID" >/dev/null 2>&1 || true
    docker stop "$WORKER_METADATA_ID" >/dev/null 2>&1 || true
    docker stop "$WORKER_TRANSFER_ID" >/dev/null 2>&1 || true
    docker stop "$WORKER_SCANNER_ID" >/dev/null 2>&1 || true
    docker stop "$WORKER_FFMPEG_ID" >/dev/null 2>&1 || true
    echo ">>> Stopped."
}

trap cleanup SIGINT SIGTERM

echo ""
echo ">>> Setup Complete!"
echo "Backend API: http://localhost:8080"
echo "Frontend UI: http://localhost"
echo "Press CTRL+C to stop."

# Wait for both containers to exit (or script interruption)
# We wait on the logs to keep the script running and show output
# This is a bit tricky with two containers. 
# Simple approach: wait endlessly.
sleep infinity
