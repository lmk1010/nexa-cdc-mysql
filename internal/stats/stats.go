// Package stats —— 运行时统计 + prometheus 指标 + _canal_stats 表兼容写入。
//
// _canal_stats 表结构和原 canal-consumer 一致，agent 的 /ops/canal-status 不用改：
//   id BIGINT PK, ts DATETIME(3), hostname VARCHAR(64), table_name VARCHAR(64),
//   ins_count INT, upd_count INT, del_count INT, err_count INT,
//   last_apply_id VARCHAR(128), last_apply_at DATETIME(3)
package stats

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Table 单表在一个 flush 窗口内的统计。
type Table struct {
	Ins         int
	Upd         int
	Del         int
	Err         int
	LastApplyAt time.Time
	LastApplyID string
}

// Stats 全局统计（无锁热路径 + 定期 flush）。
type Stats struct {
	mu           sync.Mutex
	tables       map[string]*Table
	TotalBatches uint64
	TotalRows    uint64
	TotalErrors  uint64
	LastBatchAt  time.Time
	StartedAt    time.Time

	// 位点
	BinlogFile string
	BinlogPos  uint32

	// 连接状态
	Connected     bool
	LastConnected time.Time
	Reconnects    uint64

	// prometheus
	promInsCounter *prometheus.CounterVec
	promUpdCounter *prometheus.CounterVec
	promDelCounter *prometheus.CounterVec
	promErrCounter *prometheus.CounterVec
	promRowsGauge  prometheus.Gauge
	promPosGauge   prometheus.Gauge
	promConnGauge  prometheus.Gauge
	promReconnCnt  prometheus.Counter
}

func New(promReg *prometheus.Registry) *Stats {
	s := &Stats{
		tables:    map[string]*Table{},
		StartedAt: time.Now(),
	}
	s.promInsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "nexa_cdc_ins_total", Help: "insert events applied to sink"}, []string{"table"})
	s.promUpdCounter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "nexa_cdc_upd_total", Help: "update events applied"}, []string{"table"})
	s.promDelCounter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "nexa_cdc_del_total", Help: "delete events applied"}, []string{"table"})
	s.promErrCounter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "nexa_cdc_err_total", Help: "errors while applying"}, []string{"table"})
	s.promRowsGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "nexa_cdc_last_batch_rows", Help: "rows in last batch"})
	s.promPosGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "nexa_cdc_binlog_position", Help: "current binlog position"})
	s.promConnGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "nexa_cdc_connected", Help: "1 if syncer is connected to source"})
	s.promReconnCnt = prometheus.NewCounter(prometheus.CounterOpts{Name: "nexa_cdc_reconnects_total", Help: "total reconnect attempts"})
	if promReg != nil {
		promReg.MustRegister(s.promInsCounter, s.promUpdCounter, s.promDelCounter, s.promErrCounter,
			s.promRowsGauge, s.promPosGauge, s.promConnGauge, s.promReconnCnt)
	}
	return s
}

// Record 记录一次单表的写入（热路径，加锁尽量短）。
func (s *Stats) Record(table string, ins, upd, del, err int, applyID string) {
	s.mu.Lock()
	t := s.tables[table]
	if t == nil {
		t = &Table{}
		s.tables[table] = t
	}
	t.Ins += ins
	t.Upd += upd
	t.Del += del
	t.Err += err
	t.LastApplyAt = time.Now()
	if applyID != "" {
		t.LastApplyID = applyID
	}
	s.TotalRows += uint64(ins + upd + del)
	s.TotalErrors += uint64(err)
	s.LastBatchAt = t.LastApplyAt
	s.mu.Unlock()

	if ins > 0 {
		s.promInsCounter.WithLabelValues(table).Add(float64(ins))
	}
	if upd > 0 {
		s.promUpdCounter.WithLabelValues(table).Add(float64(upd))
	}
	if del > 0 {
		s.promDelCounter.WithLabelValues(table).Add(float64(del))
	}
	if err > 0 {
		s.promErrCounter.WithLabelValues(table).Add(float64(err))
	}
	s.promRowsGauge.Set(float64(ins + upd + del))
}

// SetPosition 更新当前 binlog 位点（供 flush 到 _canal_stats）。
func (s *Stats) SetPosition(file string, pos uint32) {
	s.mu.Lock()
	s.BinlogFile = file
	s.BinlogPos = pos
	s.mu.Unlock()
	s.promPosGauge.Set(float64(pos))
}

// Position 原子读取当前位点，用于持久化到 position.yaml。
// 保证 file 和 pos 是同一时刻的快照（同一把锁下取）。
func (s *Stats) Position() (file string, pos uint32) {
	s.mu.Lock()
	file, pos = s.BinlogFile, s.BinlogPos
	s.mu.Unlock()
	return
}

func (s *Stats) SetConnected(up bool) {
	s.mu.Lock()
	if up && !s.Connected {
		s.LastConnected = time.Now()
	}
	s.Connected = up
	s.mu.Unlock()
	if up {
		s.promConnGauge.Set(1)
	} else {
		s.promConnGauge.Set(0)
	}
}

func (s *Stats) IncReconnect() {
	s.mu.Lock()
	s.Reconnects++
	s.mu.Unlock()
	s.promReconnCnt.Inc()
}

// SnapshotAndReset 取当前 tables 累积值并清零 —— 用于每 60s flush 到 _canal_stats。
// 返回 (副本, snapshotAt)。
func (s *Stats) SnapshotAndReset() (map[string]Table, time.Time) {
	s.mu.Lock()
	out := make(map[string]Table, len(s.tables))
	for k, v := range s.tables {
		out[k] = *v
		v.Ins, v.Upd, v.Del, v.Err = 0, 0, 0, 0
	}
	t := s.LastBatchAt
	s.mu.Unlock()
	return out, t
}

// Snapshot 只取不清零 —— 用于 /stats HTTP endpoint。
func (s *Stats) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	tables := make(map[string]Table, len(s.tables))
	for k, v := range s.tables {
		tables[k] = *v
	}
	return map[string]any{
		"started_at":     s.StartedAt.Format(time.RFC3339),
		"total_batches":  s.TotalBatches,
		"total_rows":     s.TotalRows,
		"total_errors":   s.TotalErrors,
		"last_batch_at":  s.LastBatchAt.Format(time.RFC3339),
		"binlog_file":    s.BinlogFile,
		"binlog_pos":     s.BinlogPos,
		"connected":      s.Connected,
		"last_connected": s.LastConnected.Format(time.RFC3339),
		"reconnects":     s.Reconnects,
		"tables":         tables,
	}
}

func (s *Stats) IncBatch() {
	s.mu.Lock()
	s.TotalBatches++
	s.mu.Unlock()
}
