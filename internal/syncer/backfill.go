// 断档补数：binlog 接不上时，按时间列把断档期间变更过的行从源库补回 warehouse。
//
// 对源库（生产 CDB）的压力控制：
//   - 只读、单连接、串行；
//   - 按主键分窗口扫：每条 SQL 最多看 window_rows 行（默认 20000），不会出现一条大查询扫全表；
//   - 只有 create_time 的追加型表：从 warehouse 里该表已有的最大主键往后读，只扫新行；
//   - 分年表（xxx_2024）早于断档年份的直接跳过；
//   - 每个窗口之间 sleep；可配置只在夜间窗口（Asia/Shanghai）跑，白天自动暂停。
//
// 进度落盘到 position 文件旁的 backfill.json，重启接着补。
//
// 已知限制：断档期间源库被物理删除的行补不回来（业务以软删为主，影响小）。
package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

type bfTable struct {
	Cursor string `json:"cursor"`
	Done   bool   `json:"done"`
	Rows   int64  `json:"rows"`
	Mode   string `json:"mode"`
	Note   string `json:"note,omitempty"`
}

type backfillState struct {
	Since      string              `json:"since"`
	CreatedAt  string              `json:"created_at"`
	Reason     string              `json:"reason"`
	Pass       int                 `json:"pass"`       // 0 主补；1 收尾复扫（补主补期间的竞态）
	PassStart  string              `json:"pass_start"` // 本轮开始时间（下一轮复扫的起点）
	Tables     map[string]*bfTable `json:"tables"`
	Done       bool                `json:"done"`
	FinishedAt string              `json:"finished_at,omitempty"`
}

func (s *Syncer) backfillPath() string {
	return filepath.Join(filepath.Dir(s.cfg.PositionStore.File), "backfill.json")
}

func (s *Syncer) saveBackfillState(st *backfillState) error {
	raw, _ := json.MarshalIndent(st, "", "  ")
	p := s.backfillPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (s *Syncer) loadBackfillState() (*backfillState, bool) {
	raw, err := os.ReadFile(s.backfillPath())
	if err != nil {
		return nil, false
	}
	var st backfillState
	if json.Unmarshal(raw, &st) != nil {
		return nil, false
	}
	if st.Tables == nil {
		st.Tables = map[string]*bfTable{}
	}
	return &st, true
}

// ResumeBackfill 启动时调用：有没做完的补数任务就接着跑。
func (s *Syncer) ResumeBackfill() {
	if st, ok := s.loadBackfillState(); ok && !st.Done {
		log.Printf("[backfill] resuming unfinished backfill since %s (pass %d)", st.Since, st.Pass)
		s.startBackfill()
	}
}

func (s *Syncer) startBackfill() {
	if !s.bfRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.bfRunning.Store(false)
		if err := s.runBackfill(s.ctx); err != nil && s.ctx.Err() == nil {
			log.Printf("[backfill] stopped: %v", err)
			s.reportError("__backfill__", "BACKFILL", "backfill stopped: "+err.Error())
			s.notifier.Notify("backfill-fail", "CDC 断档补数中断（重启后会接着补）："+err.Error())
		}
	}()
}

var shanghai, _ = time.LoadLocation("Asia/Shanghai")

// parseWindow "00:00-07:00" → 是否在窗口内；空字符串表示随时可跑。
func inWindow(window string, now time.Time) bool {
	window = strings.TrimSpace(window)
	if window == "" {
		return true
	}
	parts := strings.Split(window, "-")
	if len(parts) != 2 {
		return true
	}
	toMin := func(hm string) int {
		t, err := time.Parse("15:04", strings.TrimSpace(hm))
		if err != nil {
			return -1
		}
		return t.Hour()*60 + t.Minute()
	}
	a, b := toMin(parts[0]), toMin(parts[1])
	if a < 0 || b < 0 {
		return true
	}
	loc := shanghai
	if loc == nil {
		loc = time.Local
	}
	n := now.In(loc)
	m := n.Hour()*60 + n.Minute()
	if a <= b {
		return m >= a && m < b
	}
	return m >= a || m < b // 跨零点，如 23:00-07:00
}

// waitWindow 不在窗口内就睡到窗口内（每分钟看一次）。
func (s *Syncer) waitWindow(ctx context.Context) error {
	logged := false
	for !inWindow(s.cfg.GapRecovery.Window, time.Now()) {
		if !logged {
			log.Printf("[backfill] outside window %s, paused", s.cfg.GapRecovery.Window)
			s.stats.SetBackfill("paused: outside window " + s.cfg.GapRecovery.Window)
			logged = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Minute):
		}
	}
	return nil
}

var yearSuffix = regexp.MustCompile(`_(\d{4})$`)

type srcTableInfo struct {
	schema, table, target string
	cols                  []string
	pk                    string
	timeCol               string
	rowsEst               int64
}

// listSourceTables 源库里命中 include 的物理表。
func (s *Syncer) listSourceTables(ctx context.Context, sinceYear int) ([]srcTableInfo, error) {
	db, err := s.sourceDataDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx,
		`SELECT table_schema, table_name, IFNULL(table_rows, 0) FROM information_schema.tables
          WHERE table_type='BASE TABLE' AND table_schema NOT IN ('mysql','sys','information_schema','performance_schema')`)
	if err != nil {
		return nil, err
	}
	type tn struct {
		schema, table string
		est           int64
	}
	var all []tn
	for rows.Next() {
		var a tn
		if rows.Scan(&a.schema, &a.table, &a.est) == nil {
			all = append(all, a)
		}
	}
	rows.Close()

	var out []srcTableInfo
	for _, t := range all {
		st := t.schema + "." + t.table
		if !s.tableIncluded(st) {
			continue
		}
		if m := yearSuffix.FindStringSubmatch(t.table); m != nil {
			if y, _ := strconv.Atoi(m[1]); y > 2000 && y < sinceYear {
				continue // 往年分表，断档期间不会有新数据
			}
		}
		sch, err := s.schema.Get(t.schema, t.table)
		if err != nil {
			continue
		}
		info := srcTableInfo{schema: t.schema, table: t.table, target: s.normalizeTable(st), cols: sch.Columns, rowsEst: t.est}
		if len(sch.PKIdx) == 1 {
			info.pk = sch.Columns[sch.PKIdx[0]]
		}
		for _, c := range sch.Columns {
			lc := strings.ToLower(c)
			if lc == "update_time" {
				info.timeCol = c
			}
		}
		if info.timeCol == "" {
			for _, c := range sch.Columns {
				if strings.ToLower(c) == "create_time" {
					info.timeCol = c
				}
			}
		}
		out = append(out, info)
	}
	return out, nil
}

func (s *Syncer) runBackfill(ctx context.Context) error {
	st, ok := s.loadBackfillState()
	if !ok || st.Done {
		return nil
	}
	since, err := time.ParseInLocation("2006-01-02 15:04:05", st.Since, time.Local)
	if err != nil {
		return fmt.Errorf("bad since %q", st.Since)
	}

	for st.Pass < 2 {
		if st.PassStart == "" {
			st.PassStart = s.sourceNow(ctx)
			_ = s.saveBackfillState(st)
		}
		passSince := st.Since
		if st.Pass == 1 {
			// 收尾复扫：主补开始后又变过的行（主补读到旧值、binlog 已写新值的竞态）
			passSince = st.prevPassStart()
		}
		tables, err := s.listSourceTables(ctx, since.Year())
		if err != nil {
			return err
		}
		for _, t := range tables {
			key := fmt.Sprintf("p%d:%s.%s", st.Pass, t.schema, t.table)
			bt := st.Tables[key]
			if bt == nil {
				bt = &bfTable{}
				st.Tables[key] = bt
			}
			if bt.Done {
				continue
			}
			if err := s.backfillTable(ctx, st, key, bt, t, passSince); err != nil {
				return err
			}
		}
		st.Pass++
		if st.Pass == 1 {
			st.Tables["_pass0_start"] = &bfTable{Cursor: st.PassStart, Done: true, Mode: "meta"}
		}
		st.PassStart = ""
		_ = s.saveBackfillState(st)
	}
	st.Done = true
	st.FinishedAt = time.Now().Format(time.RFC3339)
	_ = s.saveBackfillState(st)
	var total int64
	for k, t := range st.Tables {
		if !strings.HasPrefix(k, "_") {
			total += t.Rows
		}
	}
	msg := fmt.Sprintf("CDC 断档补数完成：自 %s 起共补 %d 行", st.Since, total)
	log.Printf("[backfill] %s", msg)
	s.stats.SetBackfill("done: " + msg)
	s.reportError("__backfill__", "BACKFILL", msg)
	s.notifier.Notify("backfill-done", msg)
	return nil
}

func (st *backfillState) prevPassStart() string {
	if m := st.Tables["_pass0_start"]; m != nil && m.Cursor != "" {
		if t, err := time.ParseInLocation("2006-01-02 15:04:05", m.Cursor, time.Local); err == nil {
			return t.Add(-5 * time.Minute).Format("2006-01-02 15:04:05")
		}
	}
	return st.Since
}

// backfillTable 单表补数。
func (s *Syncer) backfillTable(ctx context.Context, st *backfillState, key string, bt *bfTable, t srcTableInfo, since string) error {
	cfg := s.cfg.GapRecovery
	db, err := s.sourceDataDB()
	if err != nil {
		return err
	}
	fq := quoteIdent(t.schema) + "." + quoteIdent(t.table)
	save := func() { _ = s.saveBackfillState(st) }

	if t.pk == "" {
		bt.Done, bt.Mode, bt.Note = true, "skip", "no single-column primary key"
		s.reportError(t.target, "BACKFILL", fmt.Sprintf("skip %s: %s", fq, bt.Note))
		save()
		return nil
	}
	mode := "time"
	if t.timeCol == "" {
		if t.rowsEst > int64(cfg.FullCopyMaxRows) {
			bt.Done, bt.Mode, bt.Note = true, "skip", "no time column and too large for full copy"
			s.reportError(t.target, "BACKFILL", fmt.Sprintf("skip %s: %s (~%d rows)", fq, bt.Note, t.rowsEst))
			save()
			return nil
		}
		mode = "full"
	}
	// 只有 create_time 的追加型表：直接从 warehouse 已有最大主键往后读
	if mode == "time" && !strings.EqualFold(t.timeCol, "update_time") && bt.Cursor == "" {
		var maxPK []byte
		_ = s.writer.DB().QueryRowContext(ctx, fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(t.pk), quoteIdent(t.target))).Scan(&maxPK)
		if len(maxPK) > 0 {
			bt.Cursor = string(maxPK)
			mode = "append"
		}
	}
	bt.Mode = mode
	log.Printf("[backfill] %s → %s mode=%s since=%s cursor=%q", fq, t.target, mode, since, bt.Cursor)

	window := cfg.WindowRows
	for {
		if err := s.waitWindow(ctx); err != nil {
			return err
		}
		s.stats.SetBackfill(fmt.Sprintf("running %s (%s) cursor=%s rows=%d", fq, mode, bt.Cursor, bt.Rows))

		// 1) 找本窗口上界：从游标往后数 window 行的主键（只走主键，扫描量有上界）
		var upper []byte
		var q string
		var args []any
		if bt.Cursor == "" {
			q = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT 1 OFFSET %d", quoteIdent(t.pk), fq, quoteIdent(t.pk), window-1)
		} else {
			q = fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT 1 OFFSET %d", quoteIdent(t.pk), fq, quoteIdent(t.pk), quoteIdent(t.pk), window-1)
			args = append(args, bt.Cursor)
		}
		err := db.QueryRowContext(ctx, q, args...).Scan(&upper)
		last := false
		if err != nil {
			// 不够一个窗口：读到表尾
			last = true
			upper = nil
		}

		// 2) 窗口内按时间过滤取行
		conds := []string{}
		args = args[:0]
		if bt.Cursor != "" {
			conds = append(conds, quoteIdent(t.pk)+" > ?")
			args = append(args, bt.Cursor)
		}
		if upper != nil {
			conds = append(conds, quoteIdent(t.pk)+" <= ?")
			args = append(args, string(upper))
		}
		if mode == "time" {
			conds = append(conds, quoteIdent(t.timeCol)+" >= ?")
			args = append(args, since)
		}
		where := ""
		if len(conds) > 0 {
			where = " WHERE " + strings.Join(conds, " AND ")
		}
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s%s ORDER BY %s", fq, where, quoteIdent(t.pk)), args...)
		if err != nil {
			return fmt.Errorf("read %s: %w", fq, err)
		}
		cols, data, err := scanRows(rows)
		rows.Close()
		if err != nil {
			return fmt.Errorf("scan %s: %w", fq, err)
		}
		for i := 0; i < len(data); i += cfg.ChunkSize {
			j := i + cfg.ChunkSize
			if j > len(data) {
				j = len(data)
			}
			if err := s.writer.UpsertRows(ctx, t.target, cols, data[i:j]); err != nil {
				return fmt.Errorf("write %s: %w", t.target, err)
			}
		}
		bt.Rows += int64(len(data))
		if upper != nil {
			bt.Cursor = string(upper)
		}
		if last {
			bt.Done = true
			save()
			log.Printf("[backfill] %s done, %d rows", fq, bt.Rows)
			return nil
		}
		save()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(cfg.SleepMs) * time.Millisecond):
		}
	}
}
