package position

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestStoreSaveLoad(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "pos.yaml")
	s := NewStore(f)

	// 初始文件不存在 → Load 失败
	if _, ok := s.LoadFile(); ok {
		t.Fatal("expected LoadFile to fail on missing file")
	}

	// 保存 → 读回来一致
	want := mysql.Position{Name: "mysql-bin.000184", Pos: 12345}
	if err := s.SaveFile(want); err != nil {
		t.Fatalf("SaveFile: %v", err)
	}
	got, ok := s.LoadFile()
	if !ok {
		t.Fatal("LoadFile after save returned !ok")
	}
	if got.Name != want.Name || got.Pos != want.Pos {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, want)
	}

	// 覆写：pos 前进 → 读回新值
	want2 := mysql.Position{Name: "mysql-bin.000184", Pos: 99999}
	if err := s.SaveFile(want2); err != nil {
		t.Fatalf("SaveFile overwrite: %v", err)
	}
	got, _ = s.LoadFile()
	if got.Pos != want2.Pos {
		t.Errorf("overwrite: got pos %d want %d", got.Pos, want2.Pos)
	}
}

func TestStoreSaveCreatesDir(t *testing.T) {
	// 目录不存在时 SaveFile 应自动创建（生产上 /var/lib/nexa-cdc 首次运行）
	dir := t.TempDir()
	f := filepath.Join(dir, "nested", "sub", "pos.yaml")
	s := NewStore(f)
	if err := s.SaveFile(mysql.Position{Name: "x", Pos: 1}); err != nil {
		t.Fatalf("SaveFile with missing parent dir: %v", err)
	}
	if _, err := os.Stat(f); err != nil {
		t.Errorf("file not created: %v", err)
	}
}

func TestStoreLoadCorruptFile(t *testing.T) {
	// 坏 JSON → LoadFile 返回 !ok 而不是 panic
	dir := t.TempDir()
	f := filepath.Join(dir, "pos.yaml")
	if err := os.WriteFile(f, []byte("not valid json {{{"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewStore(f)
	if _, ok := s.LoadFile(); ok {
		t.Error("expected LoadFile !ok on corrupt file")
	}
}
