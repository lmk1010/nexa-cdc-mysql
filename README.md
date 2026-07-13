# nexa-cdc-mysql

> **MySQL → MySQL warehouse CDC in 300 lines of Go. Uses 30MB memory. Zero JVM landmines.**

A single-binary, single-config drop-in replacement for canal-server + canal-consumer.

## Why

Canal is a great tool for large-scale CDC — but its Java-on-Docker footprint (500MB-2GB memory, hardcoded JVM opts, silent JVM crashes, PID files, ulimit fd-table OOM…) makes it painful on small servers.

For most small-to-medium businesses (< 100 GB DBs, < 50 tables), a **single Go binary** does the same job with 30MB memory and none of the operational landmines.

## Features

- ✅ **Read MySQL binlog** directly via [go-mysql-org/go-mysql/replication](https://github.com/go-mysql-org/go-mysql)
- ✅ **Batch atomic writes** to a warehouse MySQL — a transaction's multi-table events land in one commit
- ✅ **Sharded-table normalization** — `t_order_revoke_0..N` → `t_order_revoke`
- ✅ **Idle reconnect** — 60s no events → drop + reconnect (bypasses silent-server bugs)
- ✅ **Position persistence** — file + warehouse dual-write, survives crashes
- ✅ **Full-table initial load** — first time seeing a table? SELECT it fully, then switch to binlog
- ✅ **Prometheus /metrics + /health** — ready for scraping
- ✅ **`_canal_stats` compatible schema** — plugs into existing observability
- ✅ **Whitelist DDL auto-apply** — `ALTER TABLE ... ADD COLUMN` on source auto-mirrors to sink (opt-in via `sink.auto_ddl: add_column_only`); every apply audited in `_canal_ddl_applied`

## Non-goals

- HA / multi-master failover (single instance; use k8s deployment for HA)
- Sink to Kafka / Redis / ES (this is MySQL → MySQL only; use `go-mysql-transfer` if you need MQ sinks)
- Non-additive DDL replay (DROP/MODIFY/CHANGE/RENAME are **not** auto-applied — they land in `_canal_ddl_applied` as `pending` for ops review; risk of data loss is too high to automate)

## Quick start

```bash
# 1. build (mac → linux/amd64)
GOOS=linux GOARCH=amd64 go build -o dist/nexa-cdc ./cmd/nexa-cdc

# 2. copy binary + config to server
scp dist/nexa-cdc user@server:/opt/nexa-cdc/
scp config.example.yaml user@server:/opt/nexa-cdc/config.yaml

# 3. edit config.yaml then run
./nexa-cdc -c config.yaml
```

## Config

See `config.example.yaml`.

## Origin

Extracted from KYX production infra. Part of the [Nexa](https://github.com/lmk1010/nexa) enterprise-agent stack.

## License

MIT
