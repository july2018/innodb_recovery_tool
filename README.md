# InnoDB Recovery Tool for MySQL 8.0  (v2 · 重构版)

> 模仿 [undrop-for-innodb](https://github.com/twindb/undrop-for-innodb) 的思路，  
> 结合 MySQL 8.0 InnoDB 源码（`storage/innobase/`），  
> 实现对 MySQL 8.0 `.ibd` 和裸盘数据的恢复。

**核心能力：**
- 三种恢复模式：`.ibd` 文件 / `/proc/fd` 抢救 / 裸盘扫描
- **SDI 自动提取**：从 MySQL 8.0 SDI 页自动获取表结构（含库名），无需手写 `schema.json`
- **多库同名表区分**：`--database`/`--db` 参数精确过滤
- 多线程并行扫描 + 快速预扫描
- COMPACT / DYNAMIC / REDUNDANT 行格式 & Instant ADD COLUMN

## v2 重构亮点

v2 采用 **Pipeline 架构**，将代码从 2582 行精简到 1620 行，同时保持全部功能兼容。

```
PageSource   →   prescan(PageRef[])   →   recover(RecoveredRow[])   →   OutputWriter
(数据源抽象)     (快速预扫描: 只读38B)      (深度解析: 多线程)             (SQL/CSV/JSON)
```

| 特性 | v1 (旧) | v2 (新) |
|------|---------|---------|
| 代码行数 | 2582 | **1620** ↓37% |
| 扫描器数量 | 3 个独立类 | **1 个统一 RecoveryScanner** |
| I/O 抽象 | 无 | **PageSource** (文件/设备/内存) |
| 预扫描 | 每页 6B 信息 | **每页 38B** (含 index_id 等, 可直接过滤) |
| 去重 | `str(sorted(row))` | **(page_no, heap_no)** 哈希 |
| 链式回退 | 无 | 自动回退暴力扫描 |

## 快速开始

**🚀 推荐方式：SDI 自动提取（无需手写 schema.json）**

```bash
# .ibd 文件存在
python innodb_recovery.py --ibd orders.ibd --auto-schema -o recovered.sql

# 裸盘扫描（DROP TABLE 后）
python innodb_recovery.py --device /dev/vda3 --auto-schema --table orders \
    --workers 8 --relaxed -o recovered.sql

# 多库同名表
python innodb_recovery.py --device /dev/vda3 --auto-schema --table audit_logs \
    --database db_production --workers 8 --relaxed -o recovered.sql
```

**手动 schema.json（不使用 --auto-schema 时）**

```json
{
  "database": "mydb",
  "table": "orders",
  "row_format": "DYNAMIC",
  "columns": [
    {"name": "id",         "type": "bigint",        "nullable": false, "unsigned": true},
    {"name": "user_id",    "type": "int",            "nullable": false, "unsigned": true},
    {"name": "amount",     "type": "decimal(10,2)",  "nullable": true},
    {"name": "status",     "type": "varchar(32)",    "nullable": true,  "charset": "utf8mb4"},
    {"name": "created_at", "type": "datetime",       "nullable": false}
  ]
}
```

## 恢复场景

### 场景一：.ibd 文件仍在（DELETE 误删）

```bash
# 恢复所有记录（含软删除）
python innodb_recovery.py --ibd orders.ibd --schema schema.json -o recovered.sql

# 暴力扫描（链表断裂时）
python innodb_recovery.py --ibd orders.ibd --schema schema.json --brute-force -o recovered.sql
```

### 场景二：DROP TABLE（.ibd 已删除）— 裸盘扫描

```bash
# 第一步：检测设备
python innodb_recovery.py --detect-device

# 第二步：预扫描定位
python innodb_recovery.py --device /dev/vda3 --quick-scan --workers 8

# 第三步：精准恢复（按预扫描建议的 offset/length）
python innodb_recovery.py --device /dev/vda3 \
    --auto-schema --table orders \
    --offset 18300 --length 1200 \
    --workers 8 --relaxed -o recovered.sql
```

### 场景三：/proc/fd 抢救（误 rm .ibd 文件）

```bash
python innodb_recovery.py --rescue --table orders --schema schema.json -o recovered.sql
```

> 注意：DROP TABLE 会立即关闭 .ibd 句柄，/proc/fd 方案无效。请用裸盘扫描。

## 参数一览

| 参数 | 说明 |
|------|------|
| **数据源** | |
| `--ibd FILE` | .ibd 文件路径 |
| `--device DEV` | 裸块设备（需 root） |
| `--rescue` | 从 /proc/fd 抢救（需 root） |
| **通用** | |
| `--schema FILE` | table schema JSON |
| `--auto-schema` | 从 SDI 页自动提取表结构 |
| `--table NAME` | 表名过滤 |
| `--database NAME` | 库名过滤（`--db` 同义） |
| `-o FILE` | 输出文件 |
| `--format sql\|csv\|json` | 输出格式 |
| `--no-deleted` | 排除软删除记录 |
| `--brute-force` | 暴力扫描模式 |
| `--index-id N` | 按 index_id 过滤 |
| **裸盘专用** | |
| `--offset N` / `--length N` | 扫描范围（MB） |
| `--workers N` | 并行线程数 |
| `--relaxed` | 宽松检测（接受所有 INDEX/BLOB 页） |
| `--quick-scan` | 快速预扫描（只定位页，不恢复） |
| `--space-id N` | 按 space_id 过滤 |
| **辅助** | |
| `--page-info` | 页类型统计 |
| `--gen-schema TABLE` | 生成 schema 模板 |
| `--detect-device` | 检测 MySQL 数据目录设备 |
| `-v` | 详细日志 |

## Schema 字段说明

```json
{
  "database": "mydb",       // 可选，带库名输出 `mydb`.`table`
  "table": "表名",
  "row_format": "DYNAMIC",  // COMPACT / DYNAMIC / REDUNDANT
  "columns": [
    {
      "name": "列名",
      "type": "类型(长度)",    // varchar(255), int, decimal(10,2) ...
      "nullable": true,
      "unsigned": false,      // 仅整数有效
      "charset": "utf8mb4"    // 仅 char/varchar/text 有效
    }
  ]
}
```

## InnoDB 页结构（16KB）

```
┌───────────────────────────┐  0
│  FIL Header (38 bytes)    │  checksum / page_no / prev / next / LSN / type / space_id
├───────────────────────────┤  38
│  PAGE Header (56 bytes)   │  n_recs / heap_top / level / index_id
├───────────────────────────┤  94
│  Infimum + Supremum       │  系统记录
├───────────────────────────┤  120
│  User Records             │  链表顺序，delete-marked 记录仍在
│  ...                      │
├───────────────────────────┤  heap_top
│  Free Space               │
├───────────────────────────┤  16376
│  FIL Trailer (8 bytes)    │
└───────────────────────────┘  16384
```

### COMPACT 记录格式（MySQL 8.0 new-style）

```
[变长长度列表(逆序)] [NULL位图(逆序)] [5B extra] → 系统列 → 用户数据
                                        ↑
                  info_bits(1B) + heap_no|status(2B) + next_off(2B)

MySQL 8.0 新增: REC_INFO_INSTANT_FLAG=0x80 时数据区前有 instant_version
```

## MySQL 8.0 与 5.7 关键差异

| 变化点 | MySQL 5.7 | MySQL 8.0 |
|--------|-----------|-----------|
| 字典存储 | `.frm` 文件 | SDI 页（FIL_PAGE_SDI=0x0045） |
| Instant ADD COLUMN | 无 | REC_INFO_INSTANT_FLAG / REC_INFO_VERSION_FLAG |
| 行版本号 | 无 | 数据区前置 version byte |
| Checksum | innodb_fast_checksum | 默认 crc32 |

## 常见问题

**Q: 裸盘扫描 0 匹配页？**
先用 `--quick-scan` 预扫描定位数据位置（MySQL 数据通常在磁盘深处，不在开头）。

**Q: 预扫描找到候选页但没有恢复出数据？**
加 `--relaxed` 放宽检测，或 `--brute-force` 暴力扫描。检查 schema 列定义是否准确。

**Q: 多库同名表？**
不加 `--database` 时工具会列出所有候选，然后指定库名重跑：
```bash
python innodb_recovery.py --device /dev/vda3 --auto-schema --table audit_logs \
    --database db_prod --workers 8 --relaxed -o out.sql
```

**Q: /proc/fd 抢救失败？**
DROP TABLE 会立即关闭 .ibd 句柄。请在 `rm .ibd`（非 DROP TABLE）场景使用此方案。

## 注意事项

1. 只读操作，不修改任何文件
2. 建议停止 MySQL 再操作
3. Schema 列顺序、类型、nullable 必须准确
4. 超长 BLOB/TEXT 外部页暂不自动追踪
5. 裸盘扫描和 /proc/fd 需 root 权限

## 测试

```bash
python innodb_recovery_test.py
# 输出: 13 tests, All tests passed!
```

## 示例输出

```sql
-- MySQL 8.0 InnoDB Recovery Tool v2
-- Database: mydb
-- Table: orders
-- Recovered: 2026-05-29T15:00:00
-- Total rows: 1523

INSERT INTO `mydb`.`orders` (`id`, `user_id`, `amount`, `status`, `created_at`) VALUES (1, 100, '0x...', 'paid', '0x...');
-- [DELETED] INSERT INTO `mydb`.`orders` (`id`, `user_id`, `amount`, `status`, `created_at`) VALUES (2, 101, NULL, 'pending', '0x...');
```

## 源码对应关系

| 本工具 | MySQL 8.0 源码 |
|--------|---------------|
| FIL_PAGE_* | `storage/innobase/include/fil0types.h` |
| PAGE_* | `storage/innobase/include/page0types.h` |
| REC_* | `storage/innobase/rem/rec.h` |
| compact 解析 | `storage/innobase/include/rem0rec.ic` |
| REDUNDANT 解析 | `storage/innobase/rem/rem0rec.cc` |
