// 自愈相关：回源重拉、错误落库/告警、binlog 断档自动恢复。
package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/lmk1010/nexa-cdc-mysql/internal/writer"
)

// srcData 到源库的数据连接（不带 parseTime：值原样以字节取回，写 warehouse 时由 MySQL 自己转型）。
type srcData struct {
	once sync.Once
	db   *sql.DB
	err  error
}

func (s *Syncer) sourceDataDB() (*sql.DB, error) {
	s.src.once.Do(func() {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=10s&readTimeout=120s&charset=utf8mb4",
			s.cfg.Source.User, s.cfg.Source.Password, s.cfg.Source.Host, s.cfg.Source.Port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			s.src.err = err
			return
		}
		// 只开一条：补数/回源都是串行的，不给源库加并发压力
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(10 * time.Minute)
		s.src.db = db
	})
	return s.src.db, s.src.err
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// scanRows 把查询结果读成 [][]any（[]byte 或 nil）。
func scanRows(rows *sql.Rows) ([]string, [][]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]any
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make([]any, len(cols))
		for i, b := range raw {
			if b == nil {
				row[i] = nil
			} else {
				row[i] = append([]byte(nil), b...)
			}
		}
		out = append(out, row)
	}
	return cols, out, rows.Err()
}

// refetchByPK 按主键从源库读当前行写进 warehouse；源库已没有这行就删掉。
func (s *Syncer) refetchByPK(ctx context.Context, srcSchema, srcTable, target string, pkCols []string, pkVals []any) error {
	db, err := s.sourceDataDB()
	if err != nil {
		return err
	}
	if len(pkCols) == 0 {
		return fmt.Errorf("no pk")
	}
	where := make([]string, len(pkCols))
	for i, c := range pkCols {
		where[i] = quoteIdent(c) + "=?"
	}
	q := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", quoteIdent(srcSchema), quoteIdent(srcTable), strings.Join(where, " AND "))
	rows, err := db.QueryContext(ctx, q, pkVals...)
	if err != nil {
		return err
	}
	cols, data, err := scanRows(rows)
	rows.Close()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return s.writer.DeleteByPK(ctx, target, pkCols, pkVals)
	}
	return s.writer.UpsertRowsStrict(ctx, target, cols, data)
}

// refetchRow writer 的坏行兜底：写不进去的行，回源重拉当前状态再写一次。
func (s *Syncer) refetchRow(ctx context.Context, e writer.Event, cause error) error {
	if e.Schema == "" || e.OrigTable == "" || len(e.PkColumns) == 0 {
		return cause
	}
	return s.refetchByPK(ctx, e.Schema, e.OrigTable, e.Table, e.PkColumns, e.PkValues)
}

// ---- 错误落库 / 告警 ----

var errSeen sync.Map // msg -> time.Time

// reportError 同一条错误 10 分钟内只落一次 _canal_errors。
func (s *Syncer) reportError(table, kind, msg string) {
	if t, ok := errSeen.Load(msg); ok && time.Since(t.(time.Time)) < 10*time.Minute {
		return
	}
	errSeen.Store(msg, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	file, pos := s.stats.Position()
	s.writer.RecordError(ctx, table, kind, "", msg, fmt.Sprintf("%s:%d", file, pos))
}

func isBinlogPurged(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "1236") ||
		strings.Contains(m, "Could not find first log file") ||
		strings.Contains(m, "purged binary logs")
}

func isIdleTimeout(err error) bool {
	return err != nil && strings.Contains(err.Error(), "idle timeout")
}

// onRunError Run 循环里每次 runOnce 失败时调用：分类处理，返回是否需要立刻重连（不 backoff）。
func (s *Syncer) onRunError(ctx context.Context, err error) bool {
	if isIdleTimeout(err) {
		return false
	}
	if isBinlogPurged(err) {
		if s.cfg.GapRecovery.Enabled {
			if rerr := s.recoverGap(ctx); rerr != nil {
				log.Printf("[gap] recovery failed: %v", rerr)
				s.reportError("__syncer__", "GAP", "gap recovery failed: "+rerr.Error())
				s.notifier.Notify("gap-fail", "CDC binlog 断档且自动恢复失败："+rerr.Error())
				return false
			}
			return true
		}
		s.notifier.Notify("gap", "CDC binlog 位点已被源库清理（1236），需要人工处理或开启 gap_recovery")
	}
	streak := s.stats.MarkFailure(err.Error())
	s.reportError("__syncer__", "SYNC", err.Error())
	_, dur := s.stats.Failing()
	if streak >= 5 || dur >= time.Duration(s.cfg.Alert.StallMinutes)*time.Minute {
		file, pos := s.stats.Position()
		s.notifier.Notify("stall", fmt.Sprintf("CDC 连续失败 %d 次（%s），位点 %s:%d 不前进：%s",
			streak, dur.Round(time.Second), file, pos, truncate(err.Error(), 300)))
	}
	return false
}

// ---- binlog 断档恢复 ----

// gapStartTime 断档起点：warehouse 里最后一次真正写入数据的时间。
func (s *Syncer) gapStartTime(ctx context.Context) time.Time {
	var ts sql.NullString
	_ = s.writer.DB().QueryRowContext(ctx,
		`SELECT DATE_FORMAT(MAX(last_apply_at), '%Y-%m-%d %H:%i:%s') FROM _canal_stats
          WHERE table_name <> '__heartbeat__' AND (ins_count + upd_count + del_count) > 0`).Scan(&ts)
	if ts.Valid && ts.String != "" {
		if t, err := time.ParseInLocation("2006-01-02 15:04:05", ts.String, time.Local); err == nil {
			return t
		}
	}
	// 拿不到就保守补 7 天
	return time.Now().Add(-7 * 24 * time.Hour)
}

// recoverGap 位点已被源库清掉：跳到当前 tail 继续增量，同时启动后台补数。
func (s *Syncer) recoverGap(ctx context.Context) error {
	old, _ := s.posStore.LoadFile()
	tail, err := s.querySourceMasterStatus()
	if err != nil {
		return err
	}
	since := s.gapStartTime(ctx).Add(-time.Duration(s.cfg.GapRecovery.MarginMinutes) * time.Minute)
	if s.cfg.GapRecovery.SkipBackfill {
		if err := s.posStore.SaveFile(mysql.Position{Name: tail.Name, Pos: tail.Pos}); err != nil {
			return fmt.Errorf("save tail position: %w", err)
		}
		s.setBinlogFile(tail.Name)
		s.stats.SetPosition(tail.Name, tail.Pos)
		msg := fmt.Sprintf("CDC binlog 断档：%s:%d 已被源库清理，跳到 %s:%d 恢复实时同步；%s 之后到现在的数据未补（skip_backfill）",
			old.Name, old.Pos, tail.Name, tail.Pos, since.Format("2006-01-02 15:04"))
		log.Printf("[gap] %s", msg)
		s.reportError("__syncer__", "GAP", msg)
		s.notifier.Notify("gap-start", msg)
		return nil
	}
	// 先落补数任务，再改位点：中途崩了重启也能接着补
	if err := s.saveBackfillState(&backfillState{
		Since:     since.Format("2006-01-02 15:04:05"),
		CreatedAt: time.Now().Format(time.RFC3339),
		Reason:    fmt.Sprintf("binlog gap: %s:%d purged, jumped to %s:%d", old.Name, old.Pos, tail.Name, tail.Pos),
		Tables:    map[string]*bfTable{},
	}); err != nil {
		return fmt.Errorf("save backfill state: %w", err)
	}
	if err := s.posStore.SaveFile(mysql.Position{Name: tail.Name, Pos: tail.Pos}); err != nil {
		return fmt.Errorf("save tail position: %w", err)
	}
	s.setBinlogFile(tail.Name)
	s.stats.SetPosition(tail.Name, tail.Pos)
	msg := fmt.Sprintf("CDC binlog 断档自动恢复：%s:%d 已被源库清理，跳到 %s:%d 继续增量；后台补 %s 之后的变更",
		old.Name, old.Pos, tail.Name, tail.Pos, since.Format("2006-01-02 15:04"))
	log.Printf("[gap] %s", msg)
	s.reportError("__syncer__", "GAP", msg)
	s.notifier.Notify("gap-start", msg)
	s.startBackfill()
	return nil
}

// sourceNow 源库当前时间（和源库 update_time 同一时区口径，不受容器 TZ 影响）。
func (s *Syncer) sourceNow(ctx context.Context) string {
	if db, err := s.sourceDataDB(); err == nil {
		var now string
		if db.QueryRowContext(ctx, "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')").Scan(&now) == nil && now != "" {
			return now
		}
	}
	return time.Now().Format("2006-01-02 15:04:05")
}

// reconcileColumns 启动时补列：源库（当年分表/普通表）有、warehouse 没有的列自动 ADD COLUMN。
// 断档或停机期间源库加的列，DDL 事件已经错过，靠这个补上。只在 auto_ddl=add_column_only 时做。
func (s *Syncer) reconcileColumns(ctx context.Context) {
	if s.cfg.Sink.AutoDDL != "add_column_only" {
		return
	}
	tables, err := s.listSourceTables(ctx, time.Now().Year())
	if err != nil {
		log.Printf("[reconcile] list source tables failed: %v", err)
		return
	}
	srcDB, err := s.schema.SourceDB()
	if err != nil {
		return
	}
	for _, t := range tables {
		sinkCols, err := listColumnNames(ctx, s.writer.DB(), s.cfg.Sink.Database, t.target)
		if err != nil || len(sinkCols) == 0 {
			continue // warehouse 没这张表：不自动建表
		}
		srcCols, err := listColumnNames(ctx, srcDB, t.schema, t.table)
		if err != nil {
			continue
		}
		have := map[string]bool{}
		for _, c := range sinkCols {
			have[strings.ToLower(c)] = true
		}
		missing := 0
		for _, c := range srcCols {
			if !have[strings.ToLower(c)] {
				missing++
			}
		}
		if missing == 0 {
			continue
		}
		applied, skipped, failed := s.applyAddColumns(ctx, t.schema, t.table, t.target, "startup", "startup column reconcile")
		s.writer.InvalidateSinkColumns(t.target)
		log.Printf("[reconcile] %s.%s → %s: added %d column(s), skipped %d, failed %d", t.schema, t.table, t.target, applied, skipped, failed)
	}
}
