// DDL 识别 + 列定义抽取的纯函数测试。DB 相关路径靠集成场景验证。
package syncer

import (
	"errors"
	"strings"
	"testing"
)

func TestIsIgnorableQuery(t *testing.T) {
	cases := map[string]bool{
		"BEGIN":                                true,
		"begin":                                true,
		"  BEGIN  ":                            true,
		"COMMIT":                               true,
		"ROLLBACK":                             true,
		"SAVEPOINT sp1":                        true,
		"RELEASE SAVEPOINT sp1":                true,
		"SET SESSION sql_log_bin=0":            true,
		"USE ordersys":                         true,
		"FLUSH TABLES":                         true,
		"GRANT ALL ON *.* TO 'x'":              true,
		"REVOKE ALL ON *.* FROM 'x'":           true,
		"ALTER TABLE t ADD COLUMN x INT":       false,
		"CREATE TABLE t (id INT)":              false,
		"DROP TABLE t":                         false,
		"RENAME TABLE a TO b":                  false,
		"TRUNCATE TABLE t":                     false,
		"INSERT INTO t VALUES (1)":             false,
		"":                                     false, // 空串走不到 handleDDL 前会先 isAlterTable=false
	}
	for q, want := range cases {
		got := isIgnorableQuery(q)
		if got != want {
			t.Errorf("isIgnorableQuery(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestExtractAlterTarget(t *testing.T) {
	type expect struct {
		schema, table string
		ok            bool
	}
	cases := []struct {
		q, def string
		want   expect
	}{
		// 裸表名 → 用 defaultSchema
		{"ALTER TABLE t_x ADD COLUMN a INT", "ordersys", expect{"ordersys", "t_x", true}},
		{"alter table t_x add column a int", "ordersys", expect{"ordersys", "t_x", true}},
		// 反引号包裹
		{"ALTER TABLE `t_x` ADD COLUMN a INT", "ordersys", expect{"ordersys", "t_x", true}},
		// 库名 . 表名
		{"ALTER TABLE ordersys.t_x ADD COLUMN a INT", "somedefault", expect{"ordersys", "t_x", true}},
		{"ALTER TABLE `ordersys`.`t_x` ADD COLUMN a INT", "", expect{"ordersys", "t_x", true}},
		{"ALTER TABLE `ordersys`.t_x ADD COLUMN a INT", "", expect{"ordersys", "t_x", true}},
		{"ALTER TABLE ordersys.`t_x` ADD COLUMN a INT", "", expect{"ordersys", "t_x", true}},
		// IGNORE / ONLINE 前缀
		{"ALTER IGNORE TABLE t_x ADD COLUMN a INT", "d", expect{"d", "t_x", true}},
		{"ALTER ONLINE TABLE t_x ADD COLUMN a INT", "d", expect{"d", "t_x", true}},
		// 前置注释
		{"/*!50100 comment */ ALTER TABLE t_x ADD COLUMN a INT", "d", expect{"d", "t_x", true}},
		// leading whitespace
		{"\n\t  ALTER TABLE t_x ADD COLUMN a INT", "d", expect{"d", "t_x", true}},
		// 分表命名（KYX 场景）
		{"ALTER TABLE t_order_revoke_5 ADD COLUMN reason VARCHAR(255)", "ordersys", expect{"ordersys", "t_order_revoke_5", true}},
		// 混合 ADD/DROP —— 我们仍然只识别表名，是否执行由后续 diff 决定
		{"ALTER TABLE t_x ADD COLUMN a INT, DROP COLUMN b", "d", expect{"d", "t_x", true}},
		// 非 ALTER TABLE
		{"CREATE TABLE t_x (id INT)", "d", expect{"", "", false}},
		{"ALTER USER 'x'@'%' IDENTIFIED BY 'y'", "d", expect{"", "", false}},
		{"", "d", expect{"", "", false}},
	}
	for _, c := range cases {
		s, tb, ok := extractAlterTarget(c.q, c.def)
		if ok != c.want.ok || s != c.want.schema || tb != c.want.table {
			t.Errorf("extractAlterTarget(%q, %q) = (%q, %q, %v), want (%q, %q, %v)",
				c.q, c.def, s, tb, ok, c.want.schema, c.want.table, c.want.ok)
		}
	}
}

func TestExtractColumnDef(t *testing.T) {
	// 一份贴近 MySQL 8 真实输出的 SHOW CREATE TABLE
	showCreate := "CREATE TABLE `t_order` (\n" +
		"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
		"  `order_no` varchar(64) NOT NULL DEFAULT '',\n" +
		"  `state` tinyint(4) NOT NULL DEFAULT '0' COMMENT '状态',\n" +
		"  `remark` varchar(255) DEFAULT NULL COMMENT '备注, 含逗号也能挂进去',\n" +
		"  `price` decimal(10,2) DEFAULT '0.00',\n" +
		"  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
		"  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
		"  `zh_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,\n" +
		"  `is_deleted` bit(1) NOT NULL DEFAULT b'0',\n" +
		"  `last_col` int(11) NOT NULL\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4"

	cases := []struct {
		col  string
		want string
	}{
		{"id", "bigint(20) NOT NULL AUTO_INCREMENT"},
		{"order_no", "varchar(64) NOT NULL DEFAULT ''"},
		{"state", "tinyint(4) NOT NULL DEFAULT '0' COMMENT '状态'"},
		// 关键 case：DEFAULT / COMMENT 里含逗号，不能被误当成列分隔符
		{"remark", "varchar(255) DEFAULT NULL COMMENT '备注, 含逗号也能挂进去'"},
		{"price", "decimal(10,2) DEFAULT '0.00'"}, // 类型里的 (10,2) 逗号
		{"created_at", "datetime DEFAULT CURRENT_TIMESTAMP"},
		{"updated_at", "datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"},
		{"zh_name", "varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL"},
		{"is_deleted", "bit(1) NOT NULL DEFAULT b'0'"},
		// 最后一列没有末尾逗号
		{"last_col", "int(11) NOT NULL"},
	}
	for _, c := range cases {
		got, err := extractColumnDef(showCreate, c.col)
		if err != nil {
			t.Errorf("extractColumnDef(%q) err: %v", c.col, err)
			continue
		}
		if got != c.want {
			t.Errorf("extractColumnDef(%q):\n  got:  %q\n  want: %q", c.col, got, c.want)
		}
	}

	// 不存在的列
	if _, err := extractColumnDef(showCreate, "nope"); err == nil {
		t.Errorf("expected error for missing col, got nil")
	}

	// 列名含 regex 特殊字符（防守：QuoteMeta 用了没）
	showWithDot := "CREATE TABLE `t` (\n  `weird.name` int(11) NOT NULL\n)"
	got, err := extractColumnDef(showWithDot, "weird.name")
	if err != nil || got != "int(11) NOT NULL" {
		t.Errorf("regex-special col name: got=%q err=%v", got, err)
	}
}

func TestIsAlterTable(t *testing.T) {
	cases := map[string]bool{
		"ALTER TABLE t ADD COLUMN a INT":  true,
		"alter table t add column a int":  true,
		"ALTER USER 'x' IDENTIFIED BY 'y'": false,
		"ALTER DATABASE d CHARSET utf8":    false,
		"CREATE TABLE t (id INT)":          false,
		"BEGIN":                            false,
		"":                                 false,
	}
	for q, want := range cases {
		if got := isAlterTable(q); got != want {
			t.Errorf("isAlterTable(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestIsDupColumnErr(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{errors.New("Error 1060 (42S21): Duplicate column name 'x'"), true},
		{errors.New("Duplicate column name 'x'"), true},
		{errors.New("Error 1062: Duplicate entry"), false}, // 不是列，是数据主键
		{errors.New("connection refused"), false},
		{nil, false},
	}
	for _, c := range cases {
		if got := isDupColumnErr(c.err); got != c.want {
			t.Errorf("isDupColumnErr(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("abcdef", 3); got != "abc" {
		t.Errorf("truncate: got %q", got)
	}
	if got := truncate("ab", 5); got != "ab" {
		t.Errorf("truncate short: got %q", got)
	}
	// 4000 字符边界（DDL 常见）
	long := strings.Repeat("x", 5000)
	if got := truncate(long, 4000); len(got) != 4000 {
		t.Errorf("truncate 4000: len=%d", len(got))
	}
}
