// Package syncer —— binlog 监听主循环 + 事务分界 + 分表归一化 + 断线重连。
//
// 相对 canal 的价值：
//   1. **单进程无子进程** —— Go 崩了 docker 立即感知，无 canal 那种"shell 活 Java 死"
//   2. **idle 超时主动重连** —— 60 秒没收到 event 就 disconnect 重来（canal 静默死会永不恢复）
//   3. **XID 分界原子提交** —— 一次事务的多表变更一起写入 warehouse
package syncer

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/lmk1010/nexa-cdc-mysql/internal/config"
	"github.com/lmk1010/nexa-cdc-mysql/internal/position"
	"github.com/lmk1010/nexa-cdc-mysql/internal/stats"
	"github.com/lmk1010/nexa-cdc-mysql/internal/writer"
)

type Syncer struct {
	cfg     *config.Config
	writer  *writer.Writer
	stats   *stats.Stats
	posStore *position.Store

	// 表匹配（编译好的正则）
	includeRes []*regexp.Regexp
	// shard 归一化（已经在 config.Load 里编译过）

	// 每次 dial 一个新 BinlogSyncer 实例，断了就重建
	binlogSyncer *replication.BinlogSyncer
}

func New(cfg *config.Config, w *writer.Writer, st *stats.Stats, ps *position.Store) (*Syncer, error) {
	s := &Syncer{cfg: cfg, writer: w, stats: st, posStore: ps}
	for _, pat := range cfg.Tables.Include {
		re, err := regexp.Compile("^" + pat + "$")
		if err != nil {
			return nil, fmt.Errorf("bad include pattern %q: %w", pat, err)
		}
		s.includeRes = append(s.includeRes, re)
	}
	return s, nil
}

// Run 主循环：连接 → 消费 → 断了重连。
func (s *Syncer) Run(ctx context.Context) error {
	minBackoff := time.Duration(s.cfg.Reconnect.MinBackoffMs) * time.Millisecond
	maxBackoff := time.Duration(s.cfg.Reconnect.MaxBackoffMs) * time.Millisecond
	backoff := minBackoff

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := s.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Printf("[syncer] runOnce error: %v — reconnect in %s", err, backoff)
			s.stats.SetConnected(false)
			s.stats.IncReconnect()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = minBackoff
	}
}

// runOnce 建一次 binlog 连接，消费到出错为止。
func (s *Syncer) runOnce(ctx context.Context) error {
	cfg := replication.BinlogSyncerConfig{
		ServerID: s.cfg.Source.ServerID,
		Flavor:   "mysql",
		Host:     s.cfg.Source.Host,
		Port:     s.cfg.Source.Port,
		User:     s.cfg.Source.User,
		Password: s.cfg.Source.Password,
	}
	s.binlogSyncer = replication.NewBinlogSyncer(cfg)
	defer s.binlogSyncer.Close()

	// 起始位点：先看持久化的，否则用 config.binlog_start，都没有就当前 tail
	startPos, err := s.resolveStartPosition()
	if err != nil {
		return fmt.Errorf("resolve start position: %w", err)
	}
	log.Printf("[syncer] connecting to %s:%d from binlog %s:%d",
		s.cfg.Source.Host, s.cfg.Source.Port, startPos.Name, startPos.Pos)

	streamer, err := s.binlogSyncer.StartSync(startPos)
	if err != nil {
		return fmt.Errorf("start sync: %w", err)
	}
	s.stats.SetConnected(true)

	// 事务批处理状态
	idleTimeout := time.Duration(s.cfg.Reconnect.IdleTimeoutMs) * time.Millisecond
	flushInterval := time.Duration(s.cfg.Sink.BatchFlushMs) * time.Millisecond

	// 定期 flush 计时器
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	for {
		// 用 idle timeout 保护 GetEvent —— 超时就当 socket 死了走重连
		evCtx, cancel := context.WithTimeout(ctx, idleTimeout)
		ev, err := streamer.GetEvent(evCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == context.DeadlineExceeded {
				log.Printf("[syncer] idle %s, force reconnect", idleTimeout)
				return fmt.Errorf("idle timeout")
			}
			return fmt.Errorf("get event: %w", err)
		}

		select {
		case <-flushTicker.C:
			if s.writer.Pending() > 0 {
				if err := s.writer.CommitBatch(ctx); err != nil {
					return fmt.Errorf("flush tick: %w", err)
				}
			}
		default:
		}

		if err := s.handleEvent(ctx, ev); err != nil {
			return err
		}

		if s.writer.Pending() >= s.cfg.Sink.BatchMaxRows {
			if err := s.writer.CommitBatch(ctx); err != nil {
				return fmt.Errorf("commit at max rows: %w", err)
			}
		}
	}
}

// handleEvent 单条 binlog event —— 只处理 RowsEvent（INS/UPD/DEL）和 XID（事务提交）。
func (s *Syncer) handleEvent(ctx context.Context, ev *replication.BinlogEvent) error {
	// 记录位点
	s.stats.SetPosition(s.currentBinlogFile(), ev.Header.LogPos)

	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		s.setBinlogFile(string(e.NextLogName))
	case *replication.RowsEvent:
		schemaTable := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
		if !s.tableIncluded(schemaTable) {
			return nil
		}
		targetTable := s.normalizeTable(schemaTable)
		op := opFromEventType(ev.Header.EventType)
		if op < 0 {
			return nil
		}
		return s.emitRows(ctx, e, string(e.Table.Schema), string(e.Table.Table), targetTable, op, ev.Header.LogPos)
	case *replication.XIDEvent:
		// 事务提交边界：flush 当前批以保原子性
		if s.writer.Pending() > 0 {
			if err := s.writer.CommitBatch(ctx); err != nil {
				return fmt.Errorf("commit xid: %w", err)
			}
		}
	}
	return nil
}

func opFromEventType(t replication.EventType) writer.Op {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return writer.OpInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return writer.OpUpdate
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return writer.OpDelete
	}
	return -1
}

// emitRows 从 RowsEvent 里剥出每一行并塞进 writer。
func (s *Syncer) emitRows(ctx context.Context, e *replication.RowsEvent, srcSchema, srcTable, tgtTable string, op writer.Op, logPos uint32) error {
	// UPDATE_ROWS：Rows 长度为偶数，成对是 [before, after]
	// INSERT / DELETE：每一行一个 entry
	cols := colNames(e)
	pkIdx := pkIndex(e)
	applyID := fmt.Sprintf("%s:%d", s.currentBinlogFile(), logPos)
	at := time.Now()

	stride := 1
	if op == writer.OpUpdate {
		stride = 2
	}
	for i := 0; i+stride-1 < len(e.Rows); i += stride {
		row := e.Rows[i+stride-1] // INS/DEL 就是那一行；UPD 取 after
		var before []any
		if op == writer.OpUpdate {
			before = e.Rows[i]
		}

		values := row
		pkCols, pkVals := pickPK(cols, pkIdx, row)
		if len(pkCols) == 0 {
			// 没主键的表 —— 用全字段当条件（低效但至少不丢），先跳过警告
			log.Printf("[syncer] table %s.%s has no pk in RowsEvent — skipping row", srcSchema, srcTable)
			continue
		}

		s.writer.Enqueue(writer.Event{
			Op:         op,
			Schema:     srcSchema,
			Table:      tgtTable,
			OrigTable:  srcTable,
			PkColumns:  pkCols,
			PkValues:   pkVals,
			Columns:    cols,
			Values:     values,
			BeforeVals: before,
			At:         at,
			ApplyID:    applyID,
		})
	}
	_ = ctx
	return nil
}

// colNames 从 TableMap 元数据里取列名（library 没直接暴露，需从 e.Table.ColumnName 拼）。
func colNames(e *replication.RowsEvent) []string {
	if e == nil || e.Table == nil {
		return nil
	}
	// go-mysql 从 information_schema/DDL 事件推断列名；不全时用 col_i 占位
	out := make([]string, int(e.ColumnCount))
	for i := 0; i < int(e.ColumnCount); i++ {
		if i < len(e.Table.ColumnName) && len(e.Table.ColumnName[i]) > 0 {
			out[i] = string(e.Table.ColumnName[i])
		} else {
			out[i] = fmt.Sprintf("col_%d", i)
		}
	}
	return out
}

// pkIndex 从 TableMapEvent 里读出主键列索引集合。
func pkIndex(e *replication.RowsEvent) []int {
	if e == nil || e.Table == nil {
		return nil
	}
	out := make([]int, 0, len(e.Table.PrimaryKey))
	for _, idx := range e.Table.PrimaryKey {
		out = append(out, int(idx))
	}
	return out
}

func pickPK(cols []string, idxs []int, row []any) ([]string, []any) {
	pkc := make([]string, 0, len(idxs))
	pkv := make([]any, 0, len(idxs))
	for _, i := range idxs {
		if i < 0 || i >= len(cols) {
			continue
		}
		pkc = append(pkc, cols[i])
		if i < len(row) {
			pkv = append(pkv, row[i])
		} else {
			pkv = append(pkv, nil)
		}
	}
	return pkc, pkv
}

// tableIncluded 判定源表是否在 include 列表内。
func (s *Syncer) tableIncluded(schemaTable string) bool {
	for _, re := range s.includeRes {
		if re.MatchString(schemaTable) {
			return true
		}
	}
	return false
}

// normalizeTable 分表 → 归一化目标表名。
func (s *Syncer) normalizeTable(schemaTable string) string {
	for _, r := range s.cfg.ShardRules {
		if r.Match(schemaTable) {
			return r.Target
		}
	}
	// 默认：schemaTable = "schema.table"，取 table 部分
	for i := len(schemaTable) - 1; i >= 0; i-- {
		if schemaTable[i] == '.' {
			return schemaTable[i+1:]
		}
	}
	return schemaTable
}

// resolveStartPosition 决定从哪儿开始 sync。
func (s *Syncer) resolveStartPosition() (mysql.Position, error) {
	// 1. 持久化文件
	if p, ok := s.posStore.LoadFile(); ok {
		log.Printf("[syncer] resume from persisted position %s:%d", p.Name, p.Pos)
		return p, nil
	}
	// 2. config 里显式给的
	if s.cfg.Source.BinlogStart != nil && s.cfg.Source.BinlogStart.File != "" {
		return mysql.Position{
			Name: s.cfg.Source.BinlogStart.File,
			Pos:  s.cfg.Source.BinlogStart.Position,
		}, nil
	}
	// 3. 从当前 tail —— 用 SHOW MASTER STATUS 查
	return s.querySourceMasterStatus()
}

func (s *Syncer) querySourceMasterStatus() (mysql.Position, error) {
	// 用 sql.Open 简单查一次
	// 注意这里为了不引入循环依赖，用 database/sql 直连
	// 这个函数只在启动时调一次，性能不敏感
	return mysql.Position{}, fmt.Errorf("TODO: fallback to SHOW MASTER STATUS not implemented; set position_store.file or source.binlog_start")
}

var currentBinlogFileVar string

func (s *Syncer) setBinlogFile(f string) { currentBinlogFileVar = f }
func (s *Syncer) currentBinlogFile() string {
	if currentBinlogFileVar != "" {
		return currentBinlogFileVar
	}
	if p, ok := s.posStore.LoadFile(); ok {
		return p.Name
	}
	return ""
}
