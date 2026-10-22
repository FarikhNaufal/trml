# ==============================================================================
# TRML Binance USDⓈ-M Futures Intraday Trading Bot - Multi-stage Dockerfile
# ==============================================================================

# Stage 1: Build binary using official Go Alpine image
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install CA certificates and tzdata
RUN apk add --no-cache ca-certificates tzdata

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy application source code
COPY . .

# Build statically linked binary (pure Go, CGO disabled for modernc sqlite)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bot ./cmd/bot

# Stage 2: Minimal, secure runtime image
FROM alpine:3.20

WORKDIR /app

# Install CA certificates for HTTPS/WSS and tzdata for timezone management
RUN apk --no-cache add ca-certificates tzdata

# Copy compiled binary from builder
COPY --from=builder /app/bot /app/bot

# Create directory for persistent SQLite database
RUN mkdir -p /app/data

# Expose web dashboard port
EXPOSE 8080

# Default environment configuration
ENV HTTP_PORT=8080
ENV SQLITE_DB_PATH=/app/data/trml.db

# Run bot entrypoint
ENTRYPOINT ["/app/bot"]
