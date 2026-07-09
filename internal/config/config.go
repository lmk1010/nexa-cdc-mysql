// Package config —— 加载 YAML 配置 + 简单校验。
//
// 设计原则：配置项和 canal-server 保持语义对齐（source/sink/tables 三段），让
// canal 用户能 30 秒看懂。
package config

import (
	"fmt"
	"os"
	"regexp"

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
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c := &Config{}
	if err := yaml.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
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
	return nil
}
