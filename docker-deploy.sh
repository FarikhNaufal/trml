#!/usr/bin/env bash
set -e

# ==============================================================================
# TRML BOT - AUTO DEPLOYMENT SCRIPT (DOCKER)
# ==============================================================================

APP_NAME="trml-bot"
IMAGE_NAME="trml-bot:latest"
PORT="8080"

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

# 3. Ensure persistent data directory exists on host
mkdir -p ./data

# 4. Build Docker image
echo "🔨 Building Docker image ($IMAGE_NAME)..."
docker build -t "$IMAGE_NAME" .

# 5. Stop and remove existing container if running
if [ "$(docker ps -aq -f name=^/${APP_NAME}$)" ]; then
  echo "🛑 Stopping old container..."
  docker stop --time 15 "$APP_NAME" || true
  docker rm "$APP_NAME" || true
fi

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
