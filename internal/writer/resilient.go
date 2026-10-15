// 写入自愈：数据类错误不再让整条流原地重试。
//
// 2026-09 的事故：一笔事务里有一行写不进 warehouse（数据/结构问题），CommitBatch
// 整批回滚 → syncer 重连从同一位点重放 → 再失败……空转一个月直到 binlog 被清。
//
// 现在的策略：
//  1. 写之前按 warehouse 实际列做投影 —— 源库加了列、warehouse 还没有，不再报 Unknown column；
//  2. 整批失败且是「数据类错误」→ 退化为逐行提交，坏行交给 RowFailureHandler（回源库按主键重拉）；
//  3. 还不行就记 _canal_errors 并跳过这一行，流继续往前走；
//  4. 连接类/锁冲突类错误（可重试）仍然整批返回，交给上游重连重放，绝不跳行。
package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
)

// RowFailureHandler 处理单行写失败：返回 nil 表示已经修复（比如回源重拉写成功）。
type RowFailureHandler func(ctx context.Context, e Event, cause error) error

type sinkColCache struct {
	mu   sync.RWMutex
	cols map[string]map[string]bool // table -> col set；空 map 表示表不存在
}

// SetRowFailureHandler 由 syncer 注入（它持有源库连接，可以按主键回源重拉）。
func (w *Writer) SetRowFailureHandler(h RowFailureHandler) { w.onRowFailure = h }

// retryableMySQLErrors 这些错误重试大概率能好，不能跳行。
var retryableMySQLErrors = map[uint16]bool{
	1040: true, // too many connections
	1053: true, // server shutdown
	1205: true, // lock wait timeout
	1213: true, // deadlock
	1317: true, // query interrupted
	2006: true, // server has gone away
	2013: true, // lost connection
	3024: true, // max_execution_time
}

// IsRetryable 只有明确是 MySQL 返回的「数据/结构类」错误才算不可重试；
// 网络错误、context 取消、驱动 bad conn 一律当可重试。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return retryableMySQLErrors[me.Number]
	}
	return true
}

func (w *Writer) sinkColumns(ctx context.Context, table string) map[string]bool {
	w.colCache.mu.RLock()
	set, ok := w.colCache.cols[table]
	w.colCache.mu.RUnlock()
	if ok {
		return set
	}
	set = map[string]bool{}
	rows, err := w.db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema=? AND table_name=?`,
		w.cfg.Database, table)
	if err != nil {
		// 查不到就不投影（返回 nil），按原列写，失败走逐行兜底
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var c string
		if rows.Scan(&c) == nil {
			set[strings.ToLower(c)] = true
		}
	}
	w.colCache.mu.Lock()
	if w.colCache.cols == nil {
		w.colCache.cols = map[string]map[string]bool{}
	}
	w.colCache.cols[table] = set
	w.colCache.mu.Unlock()
	return set
}

// InvalidateSinkColumns DDL 补列后调用，让下次写入重新读列。
func (w *Writer) InvalidateSinkColumns(table string) {
	w.colCache.mu.Lock()
	delete(w.colCache.cols, table)
	w.colCache.mu.Unlock()
}

// project 只保留 warehouse 里存在的列。set 为空（表不存在）或 nil（查失败）时原样返回。
func project(set map[string]bool, cols []string, vals []any) ([]string, []any, []string) {
	if len(set) == 0 {
		return cols, vals, nil
	}
	outC := make([]string, 0, len(cols))
	outV := make([]any, 0, len(cols))
	var dropped []string
	for i, c := range cols {
		if set[strings.ToLower(c)] {
			outC = append(outC, c)
			if i < len(vals) {
				outV = append(outV, vals[i])
			} else {
				outV = append(outV, nil)
			}
		} else {
			dropped = append(dropped, c)
		}
	}
	return outC, outV, dropped
}

var warnedDrop sync.Map // table.col -> struct{}

// projectEvents 批量投影，返回新切片（不改原事件）。
func (w *Writer) projectEvents(ctx context.Context, events []Event) []Event {
	out := make([]Event, len(events))
	for i, e := range events {
		if e.Op == OpDelete {
			out[i] = e
			continue
		}
		set := w.sinkColumns(ctx, e.Table)
		cols, vals, dropped := project(set, e.Columns, e.Values)
		for _, c := range dropped {
			key := e.Table + "." + c
			if _, seen := warnedDrop.LoadOrStore(key, struct{}{}); !seen {
				log.Printf("[writer] column %s missing in warehouse — writing without it (auto_ddl 会补；也可手动 ALTER)", key)
			}
		}
		e.Columns, e.Values = cols, vals
		out[i] = e
	}
	return out
}

// applyOne 单行写入（自己一个事务）。
func (w *Writer) applyOne(ctx context.Context, e Event) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	switch e.Op {
	case OpInsert:
		err = execInsertBatch(ctx, tx, e.Table, e.Columns, []Event{e})
	case OpUpdate:
		err = execUpdateOne(ctx, tx, e)
	case OpDelete:
		err = execDeleteBatch(ctx, tx, e.Table, e.PkColumns, []Event{e})
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// commitRowByRow 整批失败后的逐行兜底。返回非 nil 只可能是可重试错误。
func (w *Writer) commitRowByRow(ctx context.Context, events []Event) (applied map[string][3]int, failed map[string]int, err error) {
	applied = map[string][3]int{}
	failed = map[string]int{}
	for _, e := range events {
		werr := w.applyOne(ctx, e)
		if werr == nil {
			a := applied[e.Table]
			a[e.Op]++
			applied[e.Table] = a
			continue
		}
		if IsRetryable(werr) {
			return applied, failed, werr
		}
		if w.onRowFailure != nil {
			if herr := w.onRowFailure(ctx, e, werr); herr == nil {
				a := applied[e.Table]
				a[e.Op]++
				applied[e.Table] = a
				w.RecordError(ctx, e.Table, e.Op.String(), pkString(e.PkValues), "repaired by refetch: "+werr.Error(), e.ApplyID)
				continue
			} else if IsRetryable(herr) {
				return applied, failed, herr
			}
		}
		failed[e.Table]++
		log.Printf("[writer] skip bad row %s pk=%s at %s: %v", e.Table, pkString(e.PkValues), e.ApplyID, werr)
		w.RecordError(ctx, e.Table, e.Op.String(), pkString(e.PkValues), "skipped: "+werr.Error(), e.ApplyID)
	}
	return applied, failed, nil
}

// RecordError 写一行 _canal_errors（失败只打日志，不影响主流程）。
// 兼容老 canal 时代的表结构：不写 hostname，长度按老表上限截断（event_type 8 / err_msg 1000 / sql_preview 500）。
func (w *Writer) RecordError(ctx context.Context, table, eventType, rowID, msg, preview string) {
	if _, err := w.db.ExecContext(ctx,
		`INSERT INTO _canal_errors (table_name, event_type, row_id, err_msg, sql_preview) VALUES (?, ?, ?, ?, ?)`,
		clip(table, 64), clip(eventType, 8), clip(rowID, 64), clip(msg, 1000), clip(preview, 500)); err != nil {
		log.Printf("[writer] record _canal_errors failed: %v", err)
	}
}

// clip 按字符截断（不切坏中文）。
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// UpsertRowsStrict 回源重拉用：写不进就返回错误，由调用方决定记录/跳过。
func (w *Writer) UpsertRowsStrict(ctx context.Context, table string, cols []string, rows [][]any) error {
	return w.upsertRows(ctx, table, cols, rows, true)
}

// UpsertRows 断档补数用：按 warehouse 列投影后 INSERT ... ON DUPLICATE KEY UPDATE；
// 数据类错误逐行写，坏行记 _canal_errors 跳过。
func (w *Writer) UpsertRows(ctx context.Context, table string, cols []string, rows [][]any) error {
	return w.upsertRows(ctx, table, cols, rows, false)
}

func (w *Writer) upsertRows(ctx context.Context, table string, cols []string, rows [][]any, strict bool) error {
	if len(rows) == 0 {
		return nil
	}
	set := w.sinkColumns(ctx, table)
	if set != nil && len(set) == 0 {
		return fmt.Errorf("warehouse table %s not found", table)
	}
	evs := make([]Event, 0, len(rows))
	var pc []string
	for _, r := range rows {
		c, v, _ := project(set, cols, r)
		pc = c
		evs = append(evs, Event{Op: OpInsert, Table: table, Columns: c, Values: v})
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := execInsertBatch(ctx, tx, table, pc, evs); err != nil {
		_ = tx.Rollback()
		if strict || IsRetryable(err) {
			return err
		}
		// 数据类错误：逐行写，坏行记 _canal_errors 跳过
		for _, e := range evs {
			if rerr := w.applyOne(ctx, e); rerr != nil {
				if IsRetryable(rerr) {
					return rerr
				}
				w.RecordError(ctx, table, "BACKFILL", firstVal(e.Values), "skipped: "+rerr.Error(), "")
			}
		}
		return nil
	}
	return tx.Commit()
}

func firstVal(vals []any) string {
	if len(vals) == 0 {
		return ""
	}
	return pkString(vals[:1])
}

// DeleteByPK 回源发现行已不存在时用。
func (w *Writer) DeleteByPK(ctx context.Context, table string, pkCols []string, pkVals []any) error {
	return w.applyOne(ctx, Event{Op: OpDelete, Table: table, PkColumns: pkCols, PkValues: pkVals})
}

func pkString(vals []any) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		switch t := v.(type) {
		case []byte:
			parts[i] = string(t)
		default:
			parts[i] = fmt.Sprint(t)
		}
	}
	return strings.Join(parts, ",")
}

// 确保 sql 包被引用（applyOne 用到 *sql.Tx 由 execInsertBatch 签名约束）
var _ = sql.ErrNoRows
