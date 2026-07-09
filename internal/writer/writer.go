// Package writer —— 把 binlog event 攒批 + 事务原子提交到 warehouse MySQL。
//
// 关键设计：
//   1. **一个 XID 内的所有 event 一起 commit** —— 保证事务原子性。跨表的关联变更
//      （比如一次业务操作改了 t_order + t_work_order）在 warehouse 里也一起可见。
//   2. **超过 BatchMaxRows 或 BatchFlushMs 就 commit** —— 避免峰值堆积。
//   3. **写失败自动 rollback**，上游 syncer 会重连从上次 ack 位点重发。
package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/lmk1010/nexa-cdc-mysql/internal/config"
	"github.com/lmk1010/nexa-cdc-mysql/internal/stats"
)

// Event 一条 row 级变更 —— 已按目标表归一化后的形式。
type Event struct {
	Op          Op        // INS / UPD / DEL
	Schema      string    // 源库
	Table       string    // 归一化后的目标表名
	OrigTable   string    // 原始表名（分表源）
	PkColumns   []string  // 主键列名
	PkValues    []any     // 主键值
	Columns     []string  // 所有列名（INS/UPD 用到）
	Values      []any     // 所有列值（INS 用；UPD 用作 after image）
	BeforeVals  []any     // UPD 的 before image（可选）
	At          time.Time // 事件时间
	ApplyID     string    // "binlogFile:pos"
	CommitBatch int64     // 属于哪个批（同批一起 commit）
}

type Op int

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "INS"
	case OpUpdate:
		return "UPD"
	case OpDelete:
		return "DEL"
	}
	return "?"
}

type Writer struct {
	db        *sql.DB
	cfg       *config.Sink
	stats     *stats.Stats
	pending   []Event
	nextBatch int64
}

func New(cfg *config.Sink, st *stats.Stats) (*Writer, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&charset=utf8mb4",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sink: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxConn)
	db.SetMaxIdleConns(cfg.MaxConn)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sink: %w", err)
	}
	return &Writer{db: db, cfg: cfg, stats: st}, nil
}

func (w *Writer) DB() *sql.DB { return w.db }

// Enqueue 加入一条事件到当前批。
func (w *Writer) Enqueue(e Event) {
	e.CommitBatch = w.nextBatch
	w.pending = append(w.pending, e)
}

// CommitBatch 把当前 pending 全部原子提交，然后开新批。
func (w *Writer) CommitBatch(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	events := w.pending
	w.pending = nil
	w.nextBatch++

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	// 按 (table, op) 分组，同组合并成一条 batch SQL，提高吞吐
	buckets := map[string][]Event{}
	order := []string{}
	for _, e := range events {
		key := e.Table + "|" + e.Op.String()
		if _, ok := buckets[key]; !ok {
			order = append(order, key)
		}
		buckets[key] = append(buckets[key], e)
	}

	insByTable := map[string]int{}
	updByTable := map[string]int{}
	delByTable := map[string]int{}
	errByTable := map[string]int{}
	var lastApplyID string

	for _, key := range order {
		bs := buckets[key]
		e0 := bs[0]
		var err error
		switch e0.Op {
		case OpInsert:
			err = execInsertBatch(ctx, tx, e0.Table, e0.Columns, bs)
			if err == nil {
				insByTable[e0.Table] += len(bs)
			}
		case OpUpdate:
			// UPD 一条一条走（不同行 SET 列表可能不同）
			for _, e := range bs {
				if err = execUpdateOne(ctx, tx, e); err != nil {
					break
				}
				updByTable[e.Table]++
			}
		case OpDelete:
			err = execDeleteBatch(ctx, tx, e0.Table, e0.PkColumns, bs)
			if err == nil {
				delByTable[e0.Table] += len(bs)
			}
		}
		if err != nil {
			errByTable[e0.Table] += len(bs)
			_ = tx.Rollback()
			return fmt.Errorf("apply %s to %s: %w", e0.Op, e0.Table, err)
		}
		if bs[len(bs)-1].ApplyID > lastApplyID {
			lastApplyID = bs[len(bs)-1].ApplyID
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	// 统计（一次全 commit 完再记，避免锁热点）
	w.stats.IncBatch()
	touched := map[string]bool{}
	for t := range insByTable {
		touched[t] = true
	}
	for t := range updByTable {
		touched[t] = true
	}
	for t := range delByTable {
		touched[t] = true
	}
	for t := range touched {
		w.stats.Record(t, insByTable[t], updByTable[t], delByTable[t], errByTable[t], lastApplyID)
	}
	return nil
}

func execInsertBatch(ctx context.Context, tx *sql.Tx, table string, cols []string, evs []Event) error {
	if len(cols) == 0 || len(evs) == 0 {
		return nil
	}
	colList := "`" + strings.Join(cols, "`,`") + "`"
	placeholder := "(" + strings.Repeat("?,", len(cols))
	placeholder = placeholder[:len(placeholder)-1] + ")"
	placeholders := make([]string, len(evs))
	args := make([]any, 0, len(cols)*len(evs))
	for i, e := range evs {
		placeholders[i] = placeholder
		args = append(args, e.Values...)
	}
	// ON DUPLICATE 是幂等保护：重放 binlog 时不会因为主键冲突崩
	setList := make([]string, len(cols))
	for i, c := range cols {
		setList[i] = fmt.Sprintf("`%s`=VALUES(`%s`)", c, c)
	}
	q := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		table, colList, strings.Join(placeholders, ","), strings.Join(setList, ","))
	_, err := tx.ExecContext(ctx, q, args...)
	return err
}

func execUpdateOne(ctx context.Context, tx *sql.Tx, e Event) error {
	if len(e.PkColumns) == 0 {
		return fmt.Errorf("table %s row update but no pk", e.Table)
	}
	sets := make([]string, len(e.Columns))
	for i, c := range e.Columns {
		sets[i] = fmt.Sprintf("`%s`=?", c)
	}
	wheres := make([]string, len(e.PkColumns))
	for i, c := range e.PkColumns {
		wheres[i] = fmt.Sprintf("`%s`=?", c)
	}
	q := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",
		e.Table, strings.Join(sets, ","), strings.Join(wheres, " AND "))
	args := append(append([]any{}, e.Values...), e.PkValues...)
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		// 目标行不存在 —— 大概率是初始化没跑或者跳位了。用 INSERT ... ON DUPLICATE 兜底
		return execInsertBatch(ctx, tx, e.Table, e.Columns, []Event{e})
	}
	return nil
}

func execDeleteBatch(ctx context.Context, tx *sql.Tx, table string, pkCols []string, evs []Event) error {
	if len(pkCols) == 0 || len(evs) == 0 {
		return nil
	}
	// 单主键：DELETE ... WHERE pk IN (?,?,?)
	// 复合主键：一条一条 DELETE
	if len(pkCols) == 1 {
		placeholders := make([]string, len(evs))
		args := make([]any, len(evs))
		for i, e := range evs {
			placeholders[i] = "?"
			args[i] = e.PkValues[0]
		}
		q := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` IN (%s)",
			table, pkCols[0], strings.Join(placeholders, ","))
		_, err := tx.ExecContext(ctx, q, args...)
		return err
	}
	// 多列 PK
	wheres := make([]string, len(pkCols))
	for i, c := range pkCols {
		wheres[i] = fmt.Sprintf("`%s`=?", c)
	}
	q := fmt.Sprintf("DELETE FROM `%s` WHERE %s", table, strings.Join(wheres, " AND "))
	for _, e := range evs {
		if _, err := tx.ExecContext(ctx, q, e.PkValues...); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Close() error {
	return w.db.Close()
}

func (w *Writer) Pending() int { return len(w.pending) }
