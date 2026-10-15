// Package config —— 加载 YAML 配置 + 简单校验。
//
// 设计原则：配置项和 canal-server 保持语义对齐（source/sink/tables 三段），让
// canal 用户能 30 秒看懂。
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source        Source        `yaml:"source"`
	Sink          Sink          `yaml:"sink"`
	Tables        Tables        `yaml:"tables"`
	ShardRules    []ShardRule   `yaml:"shard_normalize"`
	InitialLoad   InitialLoad   `yaml:"initial_load"`
	PositionStore PositionStore `yaml:"position_store"`
	Reconnect     Reconnect     `yaml:"reconnect"`
	HTTP          HTTP          `yaml:"http"`
	Log           Log           `yaml:"log"`
	Mode          string        `yaml:"mode"` // normal / dry-run
	GapRecovery   GapRecovery   `yaml:"gap_recovery"`
	Alert         Alert         `yaml:"alert"`
}

// GapRecovery binlog 断档（位点已被源库清掉，ERROR 1236）时的自动恢复：
// 跳到源库当前 tail 继续增量，同时后台按时间列把断档期间变更过的行从源库补回 warehouse。
type GapRecovery struct {
	Enabled bool `yaml:"enabled"`
	// SkipBackfill 只跳位点恢复实时同步，不补断档期间的数据（缺口记 _canal_errors 待人工处理）
	SkipBackfill    bool   `yaml:"skip_backfill"`
	MarginMinutes   int    `yaml:"margin_minutes"`     // 断档起点再往前多补多少分钟，默认 60
	ChunkSize       int    `yaml:"chunk_size"`         // 每次从源库拉多少行，默认 2000
	FullCopyMaxRows int    `yaml:"full_copy_max_rows"` // 没有时间列的表，行数不超过它就整表补，默认 200000
	SleepMs         int    `yaml:"sleep_ms"`           // 每个窗口之间歇多久，默认 200，别把源库/小机打满
	WindowRows      int    `yaml:"window_rows"`        // 每条 SQL 最多扫多少行主键，默认 20000
	Window          string `yaml:"window"`             // 只在这个时段补数（Asia/Shanghai），如 "09:00-18:00"；空=随时
	// AppendTables 只增不改的表（正则，匹配源表名），没有 update_time 时只从 warehouse 最大主键往后读。
	// 其余没有 update_time 的表整表重拷（旧行的修改没有时间列可判断）。
	AppendTables []string `yaml:"append_tables"`
}

// Alert 出问题时推一条消息（钉钉/企业微信机器人或任意接收 JSON 的 webhook）。
type Alert struct {
	WebhookURL   string `yaml:"webhook_url"`
	Keyword      string `yaml:"keyword"`       // 钉钉机器人安全关键词，会拼在消息开头
	StallMinutes int    `yaml:"stall_minutes"` // 连续失败、位点不前进超过多少分钟告警，默认 10
}

type Source struct {
	Host        string  `yaml:"host"`
	Port        uint16  `yaml:"port"`
	User        string  `yaml:"user"`
	Password    string  `yaml:"password"`
	ServerID    uint32  `yaml:"server_id"`
	BinlogStart *Binlog `yaml:"binlog_start"`
}

type Binlog struct {
	File     string `yaml:"file"`
	Position uint32 `yaml:"position"`
}

type Sink struct {
	Host         string `yaml:"host"`
	Port         uint16 `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Database     string `yaml:"database"`
	MaxConn      int    `yaml:"max_conn"`
	BatchMaxRows int    `yaml:"batch_max_rows"`
	BatchFlushMs int    `yaml:"batch_flush_ms"`

	// AutoDDL 决定 QueryEvent（DDL）到达时的处理策略：
	//   - "off"              什么都不做（默认，最保守）
	//   - "log_only"         只落 _canal_ddl_applied 表告警，不执行
	//   - "add_column_only"  白名单：只自动补 ADD COLUMN，其他一律 log_only 处理
	// 不支持 DROP / MODIFY / RENAME / CHANGE 自动执行 —— 有丢数据风险，一律 log。
	AutoDDL string `yaml:"auto_ddl"`
}

type Tables struct {
	Include []string `yaml:"include"`
}

type ShardRule struct {
	Pattern string `yaml:"pattern"` // 正则，匹配 schema.table 形式
	Target  string `yaml:"target"`  // 归一化后的目标表名
	re      *regexp.Regexp
}

// Match 返回是否命中 shard 归一化。schemaTable 是 "schema.table" 形式。
func (r *ShardRule) Match(schemaTable string) bool {
	if r.re == nil {
		return false
	}
	return r.re.MatchString(schemaTable)
}

type InitialLoad struct {
	Enabled   bool `yaml:"enabled"`
	ChunkSize int  `yaml:"chunk_size"`
}

type PositionStore struct {
	File        string `yaml:"file"`
	SinkDBTable string `yaml:"sink_db_table"`
}

type Reconnect struct {
	IdleTimeoutMs int `yaml:"idle_timeout_ms"`
	MinBackoffMs  int `yaml:"min_backoff_ms"`
	MaxBackoffMs  int `yaml:"max_backoff_ms"`
}

type HTTP struct {
	Addr string `yaml:"addr"`
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load 从 path 读 YAML，做简单校验并填默认值。
//
// Profile 支持：先读基础 path，再叠加 profile 文件（如果存在）。
// 约定：环境变量 NEXA_CDC_ENV=prod|dev|... 时，尝试再加载
//
//	basename.$ENV.ext（比如 config.yaml → config.prod.yaml）
//
// 覆盖：profile 里出现的字段**替换**基础值。
//
// 敏感字段兜底：source.password / sink.password 允许用环境变量覆盖，
// 让生产密码不入仓库：NEXA_CDC_SOURCE_PASSWORD / NEXA_CDC_SINK_PASSWORD。
func Load(path string) (*Config, error) {
	c := &Config{}
	if err := loadYamlInto(path, c); err != nil {
		return nil, err
	}

	if env := strings.TrimSpace(os.Getenv("NEXA_CDC_ENV")); env != "" {
		if p := profilePath(path, env); p != "" {
			if _, err := os.Stat(p); err == nil {
				if err := loadYamlInto(p, c); err != nil {
					return nil, fmt.Errorf("load profile %s: %w", p, err)
				}
			}
		}
	}

	// 环境变量兜底（密码等敏感）
	if v := os.Getenv("NEXA_CDC_SOURCE_PASSWORD"); v != "" {
		c.Source.Password = v
	}
	if v := os.Getenv("NEXA_CDC_SINK_PASSWORD"); v != "" {
		c.Sink.Password = v
	}

	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	// 预编译 shard 正则
	for i := range c.ShardRules {
		re, err := regexp.Compile(c.ShardRules[i].Pattern)
		if err != nil {
			return nil, fmt.Errorf("shard rule %d: bad regex %q: %w", i, c.ShardRules[i].Pattern, err)
		}
		c.ShardRules[i].re = re
	}
	return c, nil
}

func loadYamlInto(path string, c *Config) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, c); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

// profilePath("config.yaml", "prod") → "config.prod.yaml"
func profilePath(basePath, env string) string {
	dot := strings.LastIndex(basePath, ".")
	if dot <= 0 {
		return basePath + "." + env
	}
	return basePath[:dot] + "." + env + basePath[dot:]
}

func (c *Config) applyDefaults() {
	if c.Source.Port == 0 {
		c.Source.Port = 3306
	}
	if c.Sink.Port == 0 {
		c.Sink.Port = 3306
	}
	if c.Sink.MaxConn == 0 {
		c.Sink.MaxConn = 4
	}
	if c.Sink.BatchMaxRows == 0 {
		c.Sink.BatchMaxRows = 500
	}
	if c.Sink.BatchFlushMs == 0 {
		c.Sink.BatchFlushMs = 500
	}
	if c.Sink.AutoDDL == "" {
		c.Sink.AutoDDL = "off"
	}
	if c.InitialLoad.ChunkSize == 0 {
		c.InitialLoad.ChunkSize = 5000
	}
	if c.PositionStore.File == "" {
		c.PositionStore.File = "./position.yaml"
	}
	if c.PositionStore.SinkDBTable == "" {
		c.PositionStore.SinkDBTable = "_canal_stats"
	}
	if c.Reconnect.IdleTimeoutMs == 0 {
		c.Reconnect.IdleTimeoutMs = 60000
	}
	if c.Reconnect.MinBackoffMs == 0 {
		c.Reconnect.MinBackoffMs = 2000
	}
	if c.Reconnect.MaxBackoffMs == 0 {
		c.Reconnect.MaxBackoffMs = 30000
	}
	if c.HTTP.Addr == "" {
		c.HTTP.Addr = "0.0.0.0:6060"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
	if c.Mode == "" {
		c.Mode = "normal"
	}
	if c.GapRecovery.MarginMinutes == 0 {
		c.GapRecovery.MarginMinutes = 60
	}
	if c.GapRecovery.ChunkSize == 0 {
		c.GapRecovery.ChunkSize = 2000
	}
	if c.GapRecovery.FullCopyMaxRows == 0 {
		c.GapRecovery.FullCopyMaxRows = 5000000
	}
	if c.GapRecovery.SleepMs == 0 {
		c.GapRecovery.SleepMs = 200
	}
	if c.GapRecovery.WindowRows == 0 {
		c.GapRecovery.WindowRows = 20000
	}
	if c.Alert.StallMinutes == 0 {
		c.Alert.StallMinutes = 10
	}
}

func (c *Config) validate() error {
	if c.Source.Host == "" {
		return fmt.Errorf("source.host required")
	}
	if c.Source.User == "" {
		return fmt.Errorf("source.user required")
	}
	if c.Source.ServerID == 0 {
		return fmt.Errorf("source.server_id required (must be unique among replicas)")
	}
	if c.Sink.Host == "" {
		return fmt.Errorf("sink.host required")
	}
	if c.Sink.Database == "" {
		return fmt.Errorf("sink.database required")
	}
	if len(c.Tables.Include) == 0 {
		return fmt.Errorf("tables.include cannot be empty")
	}
	if c.Mode != "normal" && c.Mode != "dry-run" {
		return fmt.Errorf("mode must be normal or dry-run, got %q", c.Mode)
	}
	switch c.Sink.AutoDDL {
	case "off", "log_only", "add_column_only":
	default:
		return fmt.Errorf("sink.auto_ddl must be off/log_only/add_column_only, got %q", c.Sink.AutoDDL)
	}
	return nil
}
