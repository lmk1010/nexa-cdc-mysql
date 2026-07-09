// Schema cache —— 从 source 的 information_schema 拉表结构。
//
// 为什么必须自己拉：MySQL binlog 默认 binlog_row_metadata=MINIMAL，TableMapEvent
// 里没有列名和主键信息。要拿这些，要么 CDB 开 FULL（生产往往做不到），要么
// 我们自己查 information_schema。
//
// 缓存策略：
//   - 每个表第一次遇到 → 查 information_schema → 缓存
//   - DDL 事件（ALTER TABLE 等）→ 让相关表的缓存失效
//   - 缓存永不主动过期（表结构稳定，DDL 触发失效即可）
package syncer

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	"github.com/lmk1010/nexa-cdc-mysql/internal/config"
)

// TableSchema 一张表的元数据（列 + 主键索引）。
type TableSchema struct {
	Columns []string // 按 ordinal_position 排
	PKIdx   []int    // 主键在 Columns 里的下标（复合主键就多个）
}

type SchemaCache struct {
	mu     sync.RWMutex
	tables map[string]*TableSchema // key = "schema.table"
	src    *config.Source
	db     *sql.DB
	dbOnce sync.Once
	dbErr  error
}

func NewSchemaCache(src *config.Source) *SchemaCache {
	return &SchemaCache{src: src, tables: map[string]*TableSchema{}}
}

func (c *SchemaCache) getDB() (*sql.DB, error) {
	c.dbOnce.Do(func() {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?parseTime=true&timeout=5s",
			c.src.User, c.src.Password, c.src.Host, c.src.Port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			c.dbErr = err
			return
		}
		db.SetMaxOpenConns(2)
		c.db = db
	})
	return c.db, c.dbErr
}

// Get 拿一张表的 schema；miss 时查 information_schema 缓存并返。
func (c *SchemaCache) Get(schema, table string) (*TableSchema, error) {
	key := schema + "." + table
	c.mu.RLock()
	if s, ok := c.tables[key]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	db, err := c.getDB()
	if err != nil {
		return nil, err
	}

	// 拉列名（按 ordinal_position）
	colRows, err := db.Query(
		`SELECT column_name FROM information_schema.columns
         WHERE table_schema=? AND table_name=?
         ORDER BY ordinal_position`,
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("query columns %s.%s: %w", schema, table, err)
	}
	var cols []string
	for colRows.Next() {
		var name string
		if err := colRows.Scan(&name); err != nil {
			colRows.Close()
			return nil, err
		}
		cols = append(cols, name)
	}
	colRows.Close()
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s.%s has no columns (does it exist?)", schema, table)
	}

	// 拉主键列名
	pkRows, err := db.Query(
		`SELECT column_name FROM information_schema.key_column_usage
         WHERE table_schema=? AND table_name=? AND constraint_name='PRIMARY'
         ORDER BY ordinal_position`,
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("query pk %s.%s: %w", schema, table, err)
	}
	pkColNames := map[string]bool{}
	for pkRows.Next() {
		var name string
		if err := pkRows.Scan(&name); err != nil {
			pkRows.Close()
			return nil, err
		}
		pkColNames[name] = true
	}
	pkRows.Close()

	var pkIdx []int
	for i, name := range cols {
		if pkColNames[name] {
			pkIdx = append(pkIdx, i)
		}
	}

	s := &TableSchema{Columns: cols, PKIdx: pkIdx}
	c.mu.Lock()
	c.tables[key] = s
	c.mu.Unlock()
	return s, nil
}

// Invalidate 让某表缓存失效（用在 DDL 之后）。
func (c *SchemaCache) Invalidate(schema, table string) {
	c.mu.Lock()
	delete(c.tables, schema+"."+table)
	c.mu.Unlock()
}
