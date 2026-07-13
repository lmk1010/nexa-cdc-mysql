// DDL 事件处理 —— QueryEvent 到达时的白名单式 schema 补齐。
//
// 设计原则（"非常安全"）：
//  1. **只加不改**：仅 ADD COLUMN 会自动应用到 warehouse；DROP/MODIFY/CHANGE/RENAME
//     一律走 log_only，落 _canal_ddl_applied 表告警等人处理。
//  2. **不写自己的 SQL parser**：识别是不是 ALTER TABLE 仅用正则；真要生成列定义时
//     直接从 source 的 SHOW CREATE TABLE 里提取原文，MySQL 自己吐的语法一定合法。
//  3. **info_schema diff 兜底幂等**：即使同一 DDL 因分表来 N 次，或者位点重放，也
//     每次先 diff sink 已有列，只补真正缺的，不会重复 ADD。
//  4. **不阻塞流**：DDL 应用失败 → 落 _canal_ddl_applied (status='failed') → 继续；
//     下条 RowsEvent 的列数不匹配保护还在，最坏也就是那张表暂停到列补齐。
//  5. **权限**：sink 账号在 EnsureTables 时已经执行过 CREATE TABLE，天然有 DDL 权限，
//     无需新账号。
//  6. **默认关闭**：sink.auto_ddl 缺省 "off"；生产要显式打开 add_column_only。

package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
)

// alterTableRe 匹配 "ALTER [IGNORE|ONLINE] TABLE [schema.]table ..." 并抓库/表名。
// 只用于**识别 + 取表名**，不用于解析子句。
//
// 支持形式：
//   ALTER TABLE t_x ADD COLUMN ...
//   ALTER TABLE `t_x` ADD COLUMN ...
//   ALTER TABLE db.t_x ADD COLUMN ...
//   ALTER TABLE `db`.`t_x` ADD COLUMN ...
//   ALTER IGNORE TABLE ... / ALTER ONLINE TABLE ...
var alterTableRe = regexp.MustCompile(
	`(?is)^\s*(?:/\*.*?\*/\s*)*` +
		`ALTER\s+(?:IGNORE\s+|ONLINE\s+)*TABLE\s+` +
		"(?:`?([A-Za-z0-9_$]+)`?\\s*\\.\\s*)?" +
		"`?([A-Za-z0-9_$]+)`?")

// isIgnorableQuery 过滤掉 QueryEvent 里 non-DDL 的杂项：BEGIN、COMMIT、SAVEPOINT、SET 等。
func isIgnorableQuery(q string) bool {
	up := strings.ToUpper(strings.TrimSpace(q))
	// 常见事务/会话噪音
	prefixes := []string{"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE SAVEPOINT", "SET ", "USE ", "FLUSH ", "GRANT ", "REVOKE "}
	for _, p := range prefixes {
		if strings.HasPrefix(up, p) {
			return true
		}
	}
	return false
}

// isAlterTable 快速判定是不是 ALTER TABLE。
func isAlterTable(q string) bool {
	return alterTableRe.MatchString(q)
}

// extractAlterTarget 从 DDL 抓 (schema, table)；schema 为空时用 defaultSchema。
// 若无法识别返回 ok=false。
func extractAlterTarget(q, defaultSchema string) (schema, table string, ok bool) {
	m := alterTableRe.FindStringSubmatch(q)
	if len(m) < 3 {
		return "", "", false
	}
	schema, table = m[1], m[2]
	if schema == "" {
		schema = defaultSchema
	}
	if table == "" {
		return "", "", false
	}
	return schema, table, true
}

// handleDDL 是 syncer 主循环里 QueryEvent 的入口。返回 nil 永远不中断流。
//
// 语义：
//   auto_ddl == "off"              →  什么都不做
//   auto_ddl == "log_only"         →  仅落 _canal_ddl_applied，不动 warehouse
//   auto_ddl == "add_column_only"  →  ALTER TABLE 触发 info_schema diff，只补 ADD COLUMN
func (s *Syncer) handleDDL(ctx context.Context, defaultSchema, query, binlogPos string) error {
	mode := s.cfg.Sink.AutoDDL
	if mode == "" || mode == "off" {
		return nil
	}
	if isIgnorableQuery(query) {
		return nil
	}
	// 只关心 ALTER TABLE；CREATE/DROP TABLE 之类先不管，写死为 log 也没意义（表加没加不影响存量流）
	if !isAlterTable(query) {
		return nil
	}

	schema, table, ok := extractAlterTarget(query, defaultSchema)
	if !ok {
		log.Printf("[ddl] cannot extract table from DDL: %.200s", query)
		return nil
	}
	schemaTable := schema + "." + table
	// 不在同步范围内的表，DDL 完全忽略
	if !s.tableIncluded(schemaTable) {
		return nil
	}
	targetTable := s.normalizeTable(schemaTable)

	// **关键**：先把当前 pending 批 commit 掉，避免 batch 里还残留基于旧 schema 的
	// INSERT 语义（虽然 writer 用显式列名，实际不会错位，但落审计时序更干净）
	if s.writer.Pending() > 0 {
		if err := s.writer.CommitBatch(ctx); err != nil {
			log.Printf("[ddl] flush pending before DDL failed: %v — 继续 DDL 处理", err)
		}
	}

	// 缓存失效，逼下条 RowsEvent 重查 source 结构
	s.schema.Invalidate(schema, table)

	if mode == "log_only" {
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "pending", query, "", "log_only mode; not applied")
		return nil
	}

	// add_column_only 模式：走 diff 补齐
	applied, skipped, failed := s.applyAddColumns(ctx, schema, table, targetTable, binlogPos, query)
	if failed > 0 {
		log.Printf("[ddl] %s.%s → %s : applied=%d skipped=%d failed=%d",
			schema, table, targetTable, applied, skipped, failed)
	} else if applied > 0 {
		log.Printf("[ddl] %s.%s → %s : applied %d ADD COLUMN(s), skipped %d already-present",
			schema, table, targetTable, applied, skipped)
	}
	return nil
}

// applyAddColumns 做 info_schema diff，找 source 有 sink 没有的列，逐个 ADD COLUMN。
// 每次 ADD 独立执行 + 独立落审计；一条失败不影响其他列。
//
// 返回：applied 成功数、skipped 已存在或不识别的列数、failed 失败数。
func (s *Syncer) applyAddColumns(ctx context.Context, schema, table, targetTable, binlogPos, sourceStmt string) (applied, skipped, failed int) {
	srcDB, err := s.schema.SourceDB()
	if err != nil {
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "failed", sourceStmt, "", "source db: "+err.Error())
		return 0, 0, 1
	}
	sinkDB := s.writer.DB()
	sinkSchema := s.cfg.Sink.Database

	srcCols, err := listColumnNames(ctx, srcDB, schema, table)
	if err != nil {
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "failed", sourceStmt, "", "list source cols: "+err.Error())
		return 0, 0, 1
	}
	sinkCols, err := listColumnNames(ctx, sinkDB, sinkSchema, targetTable)
	if err != nil {
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "failed", sourceStmt, "", "list sink cols: "+err.Error())
		return 0, 0, 1
	}
	sinkSet := map[string]bool{}
	for _, c := range sinkCols {
		sinkSet[strings.ToLower(c)] = true
	}
	var missing []string
	for _, c := range srcCols {
		if !sinkSet[strings.ToLower(c)] {
			missing = append(missing, c)
		}
	}
	if len(missing) == 0 {
		// 分表重复 DDL / 位点重放 / 已经手动同步过 —— 都落这条 skipped 便于回查
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "skipped", sourceStmt, "", "no missing columns on sink")
		return 0, 0, 0
	}

	// 取一份 source SHOW CREATE，用于从里面拷贝原始列定义
	showCreate, err := showCreateTable(ctx, srcDB, schema, table)
	if err != nil {
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "failed", sourceStmt, "", "show create: "+err.Error())
		return 0, 0, 1
	}

	for _, col := range missing {
		def, err := extractColumnDef(showCreate, col)
		if err != nil {
			// 有可能是列名含特殊字符（KYX 场景基本不会），或 MySQL 输出格式异常
			s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "skipped", sourceStmt, "", fmt.Sprintf("cannot extract def for `%s`: %v", col, err))
			skipped++
			continue
		}
		alter := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s",
			sinkSchema, targetTable, col, def)
		if _, err := sinkDB.ExecContext(ctx, alter); err != nil {
			// 幂等兜底：并发/重复情况下可能已经存在，MySQL 返回 1060
			if isDupColumnErr(err) {
				s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "skipped", sourceStmt, alter, "col already exists (1060)")
				skipped++
				continue
			}
			s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "failed", sourceStmt, alter, err.Error())
			log.Printf("[ddl] apply failed on %s.%s: %v — stmt: %s", sinkSchema, targetTable, err, alter)
			failed++
			continue
		}
		s.recordDDLAudit(ctx, schema, table, targetTable, binlogPos, "applied", sourceStmt, alter, "")
		applied++
	}
	return applied, skipped, failed
}

// listColumnNames 从 db.information_schema 拉指定表的列名（按 ordinal_position）。
// 允许 db 直接就是 information_schema 连接，也允许是业务库连接（走 3-part 名字）。
func listColumnNames(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns
         WHERE table_schema=? AND table_name=?
         ORDER BY ordinal_position`,
		schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// showCreateTable 拿 source 的 CREATE TABLE 全文，用于逐字复制列定义。
func showCreateTable(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	q := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	var name, create string
	if err := db.QueryRowContext(ctx, q).Scan(&name, &create); err != nil {
		return "", err
	}
	return create, nil
}

// extractColumnDef 从 SHOW CREATE TABLE 里抽 `col` 那一行后半段（不含反引号列名，不含末尾逗号）。
// 相比自己拼 DEFAULT / CHARACTER SET / COLLATE / COMMENT 各种边缘 case，直接复制 MySQL 自己吐的一行更稳。
func extractColumnDef(showCreate, col string) (string, error) {
	// (?m)^\s*`col`\s+(.+?),?\s*$
	pattern := `(?m)^\s*` + "`" + regexp.QuoteMeta(col) + "`" + `\s+(.+?),?\s*$`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}
	m := re.FindStringSubmatch(showCreate)
	if len(m) < 2 {
		return "", fmt.Errorf("column line not found")
	}
	return strings.TrimSpace(m[1]), nil
}

// isDupColumnErr 检测 MySQL 1060 (Duplicate column name)。用 error 文本兜，避免引入 driver 类型依赖。
func isDupColumnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1060") || strings.Contains(msg, "Duplicate column name")
}

// recordDDLAudit 落一行 _canal_ddl_applied。永不返回错误 —— 审计写失败只 log。
func (s *Syncer) recordDDLAudit(ctx context.Context, schema, table, targetTable, binlogPos, status, sourceStmt, appliedStmt, errMsg string) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "nexa-cdc"
	}
	_, err := s.writer.DB().ExecContext(ctx,
		`INSERT INTO _canal_ddl_applied
         (hostname, src_schema, src_table, target_table, binlog_pos, status, source_stmt, applied_stmt, err_msg)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		hostname, schema, table, targetTable, binlogPos, status,
		truncate(sourceStmt, 4000), truncate(appliedStmt, 4000), truncate(errMsg, 4000))
	if err != nil {
		log.Printf("[ddl] audit write failed: %v (status=%s table=%s.%s)", err, status, schema, table)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
