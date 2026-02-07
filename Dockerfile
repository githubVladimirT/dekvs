FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o dekvs-server ./cmd/server

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/dekvs-server .
COPY --from=builder /app/proto ./proto

EXPOSE 13000 12000 9090
# CMD ["./dekvs-server", "-id", "${NODE_ID}", "-raft", "${RAFT_ADDR}", "-grpc", "${GRPC_ADDR}", "-data", "/data"]

# CMD ["./dekvs-server", 
#      "-id", "${NODE_ID}", 
#      "-raft", "${RAFT_ADDR}", 
#      "-grpc", "${GRPC_ADDR}", 
#      "-data", "/data",
#      "-join", "${JOIN_ADDR}"]

CMD ["sh", "-c", "./dekvs-server -id ${NODE_ID} -raft ${RAFT_ADDR} -grpc ${GRPC_ADDR} -data /data ${JOIN_ADDR:+-join $JOIN_ADDR}"]
