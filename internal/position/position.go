// Package position —— 位点持久化：**本地文件** + **warehouse `_canal_stats`** 双写。
//
// 双写的意义：
//   - 本地文件恢复快（进程重启 <10ms）
//   - warehouse 表落库让运维监控可看（同一个 endpoint 就能查）
package position

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type Store struct {
	filePath string
	mu       sync.Mutex
}

func NewStore(filePath string) *Store {
	return &Store{filePath: filePath}
}

type persisted struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

func (s *Store) LoadFile() (mysql.Position, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.filePath)
	if err != nil {
		return mysql.Position{}, false
	}
	var p persisted
	if err := json.Unmarshal(raw, &p); err != nil {
		return mysql.Position{}, false
	}
	if p.File == "" {
		return mysql.Position{}, false
	}
	return mysql.Position{Name: p.File, Pos: p.Pos}, true
}

func (s *Store) SaveFile(p mysql.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.filePath), 0o755); err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(persisted{File: p.Name, Pos: p.Pos}, "", "  ")
	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.filePath)
}
