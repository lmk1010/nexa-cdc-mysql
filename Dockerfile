# nexa-cdc single-binary Docker image.
#
# Build:  docker build -t nexa-cdc:latest .
# Run:    docker run -d --name nexa-cdc --restart unless-stopped \
#           -v /opt/nexa-cdc/config.yaml:/etc/nexa-cdc/config.yaml \
#           -v /var/lib/nexa-cdc:/var/lib/nexa-cdc \
#           -p 6060:6060 nexa-cdc:latest

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/nexa-cdc ./cmd/nexa-cdc

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 1000 nexa
WORKDIR /app
COPY --from=builder /out/nexa-cdc /usr/local/bin/nexa-cdc
USER nexa
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -q -O - http://127.0.0.1:6060/health >/dev/null 2>&1 || exit 1
EXPOSE 6060
ENTRYPOINT ["/usr/local/bin/nexa-cdc"]
CMD ["-c", "/etc/nexa-cdc/config.yaml"]
