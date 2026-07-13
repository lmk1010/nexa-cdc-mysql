// _canal_stats / _canal_errors 表兼容写入 —— 让原 canal-consumer 的 warehouse
// 监控数据无缝延续，agent 端 /ops/canal-status 不用改。
//
// 每 60 秒：
//   1. 上报心跳（table_name = '__heartbeat__'）—— 用于判定 consumer 存活
//   2. 每张表落一行统计（ins/upd/del/err 24h 汇总由 SQL 侧算）
//   3. 落 last_apply_id（binlogFile:pos）
package stats

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"
)

// EnsureTables 建立 _canal_stats 和 _canal_errors 表（如果不存在）—— 复用 canal
// 时代的 schema，让 kyx-agent 的 /ops/canal-status 端点无缝工作。
func EnsureTables(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS _canal_stats (
      id BIGINT NOT NULL AUTO_INCREMENT,
      ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      hostname VARCHAR(64) NOT NULL,
      table_name VARCHAR(64) NOT NULL,
      ins_count INT NOT NULL DEFAULT 0,
      upd_count INT NOT NULL DEFAULT 0,
      del_count INT NOT NULL DEFAULT 0,
      err_count INT NOT NULL DEFAULT 0,
      last_apply_id VARCHAR(128),
      last_apply_at DATETIME(3),
      PRIMARY KEY (id),
      INDEX idx_ts (ts),
      INDEX idx_table_ts (table_name, ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='CDC consumer 每分钟一行的活跃度指标'`,
		`CREATE TABLE IF NOT EXISTS _canal_errors (
      id BIGINT NOT NULL AUTO_INCREMENT,
      ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      hostname VARCHAR(64) NOT NULL,
      table_name VARCHAR(64),
      event_type VARCHAR(32),
      row_id VARCHAR(64),
      err_msg TEXT,
      sql_preview TEXT,
      PRIMARY KEY (id),
      INDEX idx_ts (ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='CDC 应用失败明细'`,
		`CREATE TABLE IF NOT EXISTS _canal_ddl_applied (
      id BIGINT NOT NULL AUTO_INCREMENT,
      ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      hostname VARCHAR(64) NOT NULL,
      src_schema VARCHAR(64) NOT NULL,
      src_table VARCHAR(64) NOT NULL,
      target_table VARCHAR(64) NOT NULL,
      binlog_pos VARCHAR(128),
      status VARCHAR(16) NOT NULL,
      source_stmt TEXT NOT NULL,
      applied_stmt TEXT,
      err_msg TEXT,
      PRIMARY KEY (id),
      INDEX idx_ts (ts),
      INDEX idx_table (src_table, ts),
      INDEX idx_status (status, ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='CDC DDL 处理审计（applied/skipped/pending/failed）'`,
	}
	for _, q := range stmts {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// StartFlushLoop 定期把 stats 落到 _canal_stats。
// 一次 flush 会：
//   1. 心跳一行（table_name=__heartbeat__）
//   2. 每张有活动的表落一行 24h 累加
//   3. 快照后清零本地累积（下次 flush 只报增量）
func StartFlushLoop(ctx context.Context, db *sql.DB, s *Stats, interval time.Duration) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "nexa-cdc"
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := flushOnce(ctx, db, s, hostname); err != nil {
					log.Printf("[stats-flush] err: %v", err)
				}
			}
		}
	}()
}

func flushOnce(ctx context.Context, db *sql.DB, s *Stats, hostname string) error {
	tables, _ := s.SnapshotAndReset()
	s.mu.Lock()
	binlogPosID := ""
	if s.BinlogFile != "" {
		binlogPosID = s.BinlogFile + ":" + itoa(int(s.BinlogPos))
	}
	s.mu.Unlock()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// 心跳
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO _canal_stats (hostname, table_name, ins_count, upd_count, del_count, err_count, last_apply_id, last_apply_at)
         VALUES (?, '__heartbeat__', 0, 0, 0, 0, ?, NOW(3))`,
		hostname, binlogPosID,
	); err != nil {
		_ = tx.Rollback()
		return err
	}
	// 每张有活动的表一行
	for name, t := range tables {
		if t.Ins == 0 && t.Upd == 0 && t.Del == 0 && t.Err == 0 {
			continue
		}
		lastAt := t.LastApplyAt
		if lastAt.IsZero() {
			lastAt = time.Now()
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO _canal_stats (hostname, table_name, ins_count, upd_count, del_count, err_count, last_apply_id, last_apply_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			hostname, name, t.Ins, t.Upd, t.Del, t.Err, t.LastApplyID, lastAt,
		); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
