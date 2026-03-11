# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server ./cmd/server

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS and grpcurl
RUN apk add --no-cache ca-certificates curl

# Install grpcurl for health checks and debugging
RUN GRPCURL_VERSION=1.9.1 && \
    wget -q https://github.com/fullstorydev/grpcurl/releases/download/v${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz && \
    tar -xzf grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz && \
    mv grpcurl /usr/local/bin/ && \
    rm grpcurl_${GRPCURL_VERSION}_linux_x86_64.tar.gz && \
    chmod +x /usr/local/bin/grpcurl

# Copy binary from builder
COPY --from=builder /app/server .

# Expose ports (gRPC, Raft, Metrics)
EXPOSE 8080 9090 19090

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD grpcurl -plaintext -max-time 5 localhost:8080 kv.KVService/Get || exit 1

# Run the server
ENTRYPOINT ["./server"]
