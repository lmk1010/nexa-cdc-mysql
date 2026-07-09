# 用预编译的 linux/amd64 binary 起 image。
#
# 本地 build binary 再 build image（快，服务器不用装 golang）：
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build \
#     -trimpath -ldflags "-s -w -X main.version=$(git rev-parse --short HEAD)" \
#     -o dist/nexa-cdc-linux-amd64 ./cmd/nexa-cdc
#   docker build --platform linux/amd64 -t nexa-cdc:latest .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -u 1000 nexa && \
    mkdir -p /etc/nexa-cdc /var/lib/nexa-cdc && \
    chown -R nexa:nexa /var/lib/nexa-cdc

COPY dist/nexa-cdc-linux-amd64 /usr/local/bin/nexa-cdc
RUN chmod +x /usr/local/bin/nexa-cdc

USER nexa
WORKDIR /var/lib/nexa-cdc

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -q -O - http://127.0.0.1:6060/health >/dev/null 2>&1 || exit 1

EXPOSE 6060
ENTRYPOINT ["/usr/local/bin/nexa-cdc"]
CMD ["-c", "/etc/nexa-cdc/config.yaml"]
