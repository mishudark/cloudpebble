FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /pebble-bigtable ./cmd/pebble-bigtable

FROM alpine:3.21

RUN apk --no-cache add ca-certificates

RUN addgroup -S cloudpebble && adduser -S cloudpebble -G cloudpebble
USER cloudpebble

WORKDIR /data

COPY --from=builder /pebble-bigtable /usr/local/bin/pebble-bigtable

EXPOSE 9000

ENTRYPOINT ["pebble-bigtable"]
CMD ["--addr", ":9000", "--data-dir", "/data/db", "--object-dir", "/data/obj"]
