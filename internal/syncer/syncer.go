// Package syncer —— binlog 监听主循环 + 事务分界 + 分表归一化 + 断线重连。
//
// 相对 canal 的价值：
//   1. **单进程无子进程** —— Go 崩了 docker 立即感知，无 canal 那种"shell 活 Java 死"
//   2. **idle 超时主动重连** —— 60 秒没收到 event 就 disconnect 重来（canal 静默死会永不恢复）
//   3. **XID 分界原子提交** —— 一次事务的多表变更一起写入 warehouse
package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"time"

	_ "github.com/go-sql-driver/mysql"

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
	schema   *SchemaCache

	// 表匹配（编译好的正则）
	includeRes []*regexp.Regexp
	// shard 归一化（已经在 config.Load 里编译过）

	// 每次 dial 一个新 BinlogSyncer 实例，断了就重建
	binlogSyncer *replication.BinlogSyncer
}

func New(cfg *config.Config, w *writer.Writer, st *stats.Stats, ps *position.Store) (*Syncer, error) {
	s := &Syncer{
		cfg: cfg, writer: w, stats: st, posStore: ps,
		schema: NewSchemaCache(&cfg.Source),
	}
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
//
// 关停语义（SIGTERM → ctx cancel）：defer 里最后一次 commitPending —— 把 pending 批
// commit 到 warehouse + 位点落盘。用独立 5s 超时 ctx 保证 cancel 后仍能写。
// 这样优雅关停零丢失；强杀（kill -9）最坏丢 batch_flush_ms 窗口内未 commit 的数据，
// 下次启动会从**上次成功 commit 的位点**续，通过 INSERT ... ON DUPLICATE KEY UPDATE 幂等重放。
func (s *Syncer) Run(ctx context.Context) error {
	defer s.finalFlush()

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
			if err := s.commitPending(ctx); err != nil {
				return fmt.Errorf("flush tick: %w", err)
			}
		default:
		}

		if err := s.handleEvent(ctx, ev); err != nil {
			return err
		}

		if s.writer.Pending() >= s.cfg.Sink.BatchMaxRows {
			if err := s.commitPending(ctx); err != nil {
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
		// 事务提交边界：flush 当前批以保原子性，位点跟着一起落盘
		if err := s.commitPending(ctx); err != nil {
			return fmt.Errorf("commit xid: %w", err)
		}
	case *replication.QueryEvent:
		// DDL / BEGIN / SET 等语句级事件。handleDDL 内部会过滤 non-DDL 噪音，
		// 并按 sink.auto_ddl 决定是否 apply。永远返回 nil：DDL 失败不重连。
		pos := fmt.Sprintf("%s:%d", s.currentBinlogFile(), ev.Header.LogPos)
		return s.handleDDL(ctx, string(e.Schema), string(e.Query), pos)
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
//
// 关键：列名和主键索引优先从 information_schema 拿（缓存），不依赖 binlog metadata。
// 因为 CDB 默认 binlog_row_metadata=MINIMAL，e.Table 里那些字段是空的。
func (s *Syncer) emitRows(ctx context.Context, e *replication.RowsEvent, srcSchema, srcTable, tgtTable string, op writer.Op, logPos uint32) error {
	// 从 information_schema 拿真实结构
	sch, err := s.schema.Get(srcSchema, srcTable)
	if err != nil {
		log.Printf("[syncer] schema fetch %s.%s failed: %v — skipping event", srcSchema, srcTable, err)
		return nil
	}
	cols := sch.Columns
	pkIdx := sch.PKIdx

	// 校验：binlog 里的列数应该 = information_schema 列数
	if int(e.ColumnCount) != len(cols) {
		log.Printf("[syncer] column count mismatch on %s.%s (binlog=%d schema=%d) — invalidating cache & skipping",
			srcSchema, srcTable, e.ColumnCount, len(cols))
		s.schema.Invalidate(srcSchema, srcTable)
		return nil
	}

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
			log.Printf("[syncer] table %s.%s has no PK — skipping row", srcSchema, srcTable)
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

// commitPending 提交 pending 批到 warehouse 并把位点落盘。
// pending 为空时不 commit，但仍会把当前观测位点写入 position.yaml
// —— 这样心跳/rotate 也能推进位点，重启不会回到很旧的地方。
func (s *Syncer) commitPending(ctx context.Context) error {
	if s.writer.Pending() > 0 {
		if err := s.writer.CommitBatch(ctx); err != nil {
			return err
		}
	}
	s.savePosition()
	return nil
}

// savePosition 把 stats 里的当前位点原子快照写到 position.yaml。
// 失败只 log 不抛 —— 位点写失败不该中断数据流；下次 commit 会再试。
func (s *Syncer) savePosition() {
	file, pos := s.stats.Position()
	if file == "" {
		return
	}
	if err := s.posStore.SaveFile(mysql.Position{Name: file, Pos: pos}); err != nil {
		log.Printf("[syncer] save position %s:%d failed: %v (non-fatal)", file, pos, err)
	}
}

// finalFlush Run 退出前的兜底：用独立超时 ctx 尝试最后一次 commit+save。
// 因为主 ctx 已经 cancel，任何用 ctx 的 DB op 会立即失败，所以必须自建 ctx。
func (s *Syncer) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.commitPending(ctx); err != nil {
		log.Printf("[syncer] final flush failed: %v — some events in last batch may need replay on restart", err)
	} else {
		file, pos := s.stats.Position()
		log.Printf("[syncer] final flush ok, position saved: %s:%d", file, pos)
	}
}

// resolveStartPosition 决定从哪儿开始 sync。优先级：
//  1. 本地 position.yaml （每次 commit 都在更新，最新，通常都走这条）
//  2. warehouse _canal_stats 里最近一条心跳的 last_apply_id （文件丢时兜底，最多 60s 旧）
//  3. config.source.binlog_start
//  4. SHOW MASTER STATUS （首次部署才走到这里，从当前 tail 开始，历史不追）
func (s *Syncer) resolveStartPosition() (mysql.Position, error) {
	if p, ok := s.posStore.LoadFile(); ok {
		log.Printf("[syncer] resume from persisted file %s:%d", p.Name, p.Pos)
		return p, nil
	}
	if p, ok := s.loadPositionFromWarehouse(); ok {
		log.Printf("[syncer] position.yaml missing — fallback to warehouse _canal_stats: %s:%d (最多 60s 旧，重放靠 ON DUPLICATE 幂等)", p.Name, p.Pos)
		return p, nil
	}
	if s.cfg.Source.BinlogStart != nil && s.cfg.Source.BinlogStart.File != "" {
		log.Printf("[syncer] no persisted position — using config binlog_start %s:%d", s.cfg.Source.BinlogStart.File, s.cfg.Source.BinlogStart.Position)
		return mysql.Position{
			Name: s.cfg.Source.BinlogStart.File,
			Pos:  s.cfg.Source.BinlogStart.Position,
		}, nil
	}
	log.Printf("[syncer] no persisted position, no config start — falling back to source tail (首次启动预期路径；如果不是首次启动说明位点丢了，会漏一段数据)")
	return s.querySourceMasterStatus()
}

// loadPositionFromWarehouse 从 warehouse 的 _canal_stats 心跳行拉最近一次位点。
// 这是 position.yaml 丢失时的兜底 —— 心跳 60s 一次，所以最多回退 60s；
// 回退窗口内的 event 会被重放，靠 INSERT ... ON DUPLICATE KEY UPDATE 幂等吸收。
func (s *Syncer) loadPositionFromWarehouse() (mysql.Position, bool) {
	row := s.writer.DB().QueryRow(
		`SELECT last_apply_id FROM _canal_stats
         WHERE table_name='__heartbeat__' AND last_apply_id IS NOT NULL AND last_apply_id != ''
         ORDER BY id DESC LIMIT 1`)
	var id string
	if err := row.Scan(&id); err != nil {
		return mysql.Position{}, false
	}
	return parseApplyID(id)
}

// parseApplyID 解析 "binlogFile:pos" 形式的 last_apply_id。
// 从后往前找冒号 —— 万一 file 名字含冒号也不会切错（虽然 MySQL binlog 命名不含）。
func parseApplyID(id string) (mysql.Position, bool) {
	colon := -1
	for i := len(id) - 1; i >= 0; i-- {
		if id[i] == ':' {
			colon = i
			break
		}
	}
	if colon <= 0 || colon == len(id)-1 {
		return mysql.Position{}, false
	}
	file := id[:colon]
	var pos uint32
	if _, err := fmt.Sscanf(id[colon+1:], "%d", &pos); err != nil {
		return mysql.Position{}, false
	}
	return mysql.Position{Name: file, Pos: pos}, true
}

// querySourceMasterStatus 用 SHOW MASTER STATUS 查源库当前 binlog tail。
// 这是**"新起 CDC"从当前位置开始**的兜底路径 —— 不会拉历史。
func (s *Syncer) querySourceMasterStatus() (mysql.Position, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s",
		s.cfg.Source.User, s.cfg.Source.Password, s.cfg.Source.Host, s.cfg.Source.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("open source: %w", err)
	}
	defer db.Close()
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, fmt.Errorf("SHOW MASTER STATUS: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return mysql.Position{}, fmt.Errorf("SHOW MASTER STATUS returned no row (binlog off?)")
	}
	cols, err := rows.Columns()
	if err != nil {
		return mysql.Position{}, err
	}
	// 前两列是 File 和 Position；其他列（Binlog_Do_DB / Ignore_DB / Executed_Gtid_Set）忽略
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return mysql.Position{}, err
	}
	var file string
	var pos uint32
	switch v := vals[0].(type) {
	case []byte:
		file = string(v)
	case string:
		file = v
	}
	switch v := vals[1].(type) {
	case int64:
		pos = uint32(v)
	case []byte:
		var p int64
		_, _ = fmt.Sscanf(string(v), "%d", &p)
		pos = uint32(p)
	case string:
		var p int64
		_, _ = fmt.Sscanf(v, "%d", &p)
		pos = uint32(p)
	}
	if file == "" {
		return mysql.Position{}, fmt.Errorf("SHOW MASTER STATUS: empty File")
	}
	log.Printf("[syncer] source tail: %s:%d", file, pos)
	return mysql.Position{Name: file, Pos: pos}, nil
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
