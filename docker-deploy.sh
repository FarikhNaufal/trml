#!/usr/bin/env bash
set -e

# ==============================================================================
# TRML BOT - AUTO DEPLOYMENT SCRIPT (DOCKER)
# ==============================================================================

APP_NAME="trml-bot"
IMAGE_NAME="trml-bot:latest"
PORT="2020"

echo "=========================================="
echo "🚀 Deploying $APP_NAME..."
echo "=========================================="

# 1. Pull latest code from GitHub main branch
echo "📥 Pulling latest changes from git main..."
git fetch origin main
git reset --hard origin/main

# 2. Check if .env exists
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    echo "⚠️  .env file not found! Copying from .env.example..."
    cp .env.example .env
    echo "⚠️  Please configure your .env file before running in production!"
  else
    echo "❌ Error: Neither .env nor .env.example found!"
    exit 1
  fi
fi

# Read HTTP_PORT from .env if defined, fallback to 2020
PORT="2020"
if [ -f .env ]; then
  ENV_PORT=$(grep -E '^[[:space:]]*HTTP_PORT=' .env | tail -n 1 | cut -d '=' -f2 | tr -d '\r\n "' || true)
  if [ -n "$ENV_PORT" ]; then
    PORT="$ENV_PORT"
  fi
fi

# 3. Ensure persistent data directory exists on host
mkdir -p ./data

# 4. Build Docker image
echo "🔨 Building Docker image ($IMAGE_NAME)..."
docker build -t "$IMAGE_NAME" .

# 5. Stop and remove existing container if running
echo "🛑 Cleaning up previous container (if any)..."
docker rm -f "$APP_NAME" 2>/dev/null || true

# 6. Run new container with persistent volume and env file
echo "▶️  Starting new container on port $PORT..."
docker run -d \
  --name "$APP_NAME" \
  --restart unless-stopped \
  -p "$PORT:$PORT" \
  --env-file .env \
  -v "$(pwd)/data:/app/data" \
  --stop-timeout 15 \
  "$IMAGE_NAME"

# 7. Verification & Status
echo "=========================================="
echo "✅ $APP_NAME successfully deployed!"
echo "=========================================="
sleep 2
docker ps --filter "name=$APP_NAME"

echo ""
echo "📜 Recent logs:"
docker logs --tail 15 "$APP_NAME"
