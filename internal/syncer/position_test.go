// 位点解析测试 —— 覆盖 warehouse 兜底路径的 last_apply_id 格式边缘 case。
package syncer

import "testing"

func TestParseApplyID(t *testing.T) {
	cases := []struct {
		id       string
		wantOK   bool
		wantFile string
		wantPos  uint32
	}{
		{"mysql-bin.000184:12345", true, "mysql-bin.000184", 12345},
		{"binlog.000001:0", true, "binlog.000001", 0}, // pos=0 合法（文件开头）
		{"mysql-bin.999999:4294967295", true, "mysql-bin.999999", 4294967295}, // uint32 边界
		// 异常输入 —— 全部返回 false，不 panic
		{"", false, "", 0},
		{":", false, "", 0},
		{":123", false, "", 0}, // 空 file
		{"file:", false, "", 0}, // 空 pos
		{"no-colon", false, "", 0},
		{"file:abc", false, "", 0}, // pos 非数字
		// 多冒号 —— 只切最后一个（防止假想的含冒号文件名）
		{"weird:file:name:999", true, "weird:file:name", 999},
	}
	for _, c := range cases {
		p, ok := parseApplyID(c.id)
		if ok != c.wantOK {
			t.Errorf("parseApplyID(%q) ok=%v want=%v", c.id, ok, c.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if p.Name != c.wantFile || p.Pos != c.wantPos {
			t.Errorf("parseApplyID(%q) = (%q, %d) want (%q, %d)",
				c.id, p.Name, p.Pos, c.wantFile, c.wantPos)
		}
	}
}
