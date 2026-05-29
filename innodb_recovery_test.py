#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
innodb_recovery_test.py — v2 测试
==================================
1. 生成最小 MySQL 8.0 COMPACT INDEX 页
2. 链式遍历 + 暴力扫描测试
3. SDI 解压/解析/文件提取测试
4. OutputWriter 格式测试
5. 去重测试
"""

import struct
import os
import sys
import json
import zlib
import tempfile
import logging

sys.path.insert(0, os.path.dirname(__file__))
from innodb_recovery import (
    UNIV_PAGE_SIZE, FIL_PAGE_TYPE, PAGE_TYPE_INDEX, PAGE_TYPE_SDI,
    FIL_PAGE_DATA, PAGE_HEADER_SIZE, PAGE_HEAP_TOP, PAGE_LEVEL, PAGE_INDEX_ID,
    PAGE_N_HEAP, PAGE_N_RECS,
    REC_N_NEW_EXTRA, REC_INFO_DELETED,
    REC_STATUS_ORDINARY, REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM,
    ColumnDef, RecordParser, parse_page_header, validate_index_page,
    PageHeader, RecoveredRow, TableSchema,
    OutputWriter, SDIExtractor, RecoveryScanner,
    FilePageSource, MemPageSource, PageRef,
)

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(message)s')


# ── 页构造工具 ──────────────────────────────────────────────────

def b_u8(v):  return bytes([v & 0xFF])
def b_u16(v): return struct.pack('>H', v & 0xFFFF)
def b_u32(v): return struct.pack('>I', v & 0xFFFFFFFF)
def b_u64(v): return struct.pack('>Q', v & 0xFFFFFFFFFFFFFFFF)

def encode_int(v: int, n: int, unsigned: bool = False) -> bytes:
    if unsigned:
        b = v.to_bytes(n, 'big')
    else:
        if v < 0:
            v = v + (1 << (n * 8))
        b = v.to_bytes(n, 'big')
    ba = bytearray(b)
    ba[0] ^= 0x80
    return bytes(ba)


def build_compact_record(fields, deleted=False, heap_no=2, next_offset=0):
    """构造 COMPACT 记录 extra+data"""
    nullable = [c for c, _ in fields if c.nullable]
    var_cols = [(c, v) for c, v in fields if c.is_variable()]

    # NULL bitmap (reversed)
    n_null = len(nullable)
    null_bm = bytearray((n_null + 7) // 8)
    ni = 0
    for c, v in fields:
        if c.nullable:
            if v is None:
                null_bm[ni // 8] |= (1 << (ni % 8))
            ni += 1
    null_bytes = bytes(reversed(null_bm))

    # var length list (reversed)
    var_len_list = []
    for c, v in reversed(var_cols):
        if v is None:
            var_len_list.append(0)
            continue
        raw = (str(v).encode('utf-8') if isinstance(v, str) else bytes(v))
        n = len(raw)
        if n < 128:
            var_len_list.append(n)
        else:
            var_len_list.append(0x80 | (n >> 8))
            var_len_list.append(n & 0xFF)

    # extra 5 bytes
    info = (REC_INFO_DELETED if deleted else 0)
    extra = bytes([info]) + b_u16((heap_no << 3) | REC_STATUS_ORDINARY) + b_u16(next_offset & 0xFFFF)

    # system cols
    sys_data = b'\x00' * 6 + b'\x00' * 7  # trx_id + roll_ptr

    # data
    data_parts = []
    for c, v in fields:
        if v is None:
            continue
        if c.is_variable():
            data_parts.append(str(v).encode('utf-8') if isinstance(v, str) else bytes(v))
        else:
            fl = c.fixed_len
            if c.data_type == 6:  # DATA_INT
                data_parts.append(encode_int(int(v), fl, c.unsigned))
            else:
                data_parts.append(str(v).encode('utf-8')[:fl].ljust(fl, b'\x00'))

    prefix = bytes(var_len_list) + null_bytes + extra
    return prefix, sys_data + b''.join(data_parts)


def build_test_page(records_data):
    """构造一个最小 16KB COMPACT INDEX 叶子页"""
    page = bytearray(UNIV_PAGE_SIZE)

    # FIL header
    struct.pack_into('>H', page, FIL_PAGE_TYPE, PAGE_TYPE_INDEX)
    struct.pack_into('>I', page, 4, 1)   # page_no = 1

    # PAGE header
    ph = FIL_PAGE_DATA
    struct.pack_into('>H', page, ph + 4, 0x8000 | 2)
    struct.pack_into('>H', page, ph + PAGE_LEVEL, 0)
    struct.pack_into('>Q', page, ph + PAGE_INDEX_ID, 1)

    # Infimum / Supremum
    INF = FIL_PAGE_DATA + PAGE_HEADER_SIZE + 20 + 5   # 119
    SUP = INF + 8 + 5                                  # 132

    inf_hs = (0 << 3) | REC_STATUS_INFIMUM
    inf_nx = SUP - INF  # 13
    page[INF - 5] = 0
    struct.pack_into('>H', page, INF - 4, inf_hs)
    struct.pack_into('>H', page, INF - 2, inf_nx & 0xFFFF)
    page[INF:INF + 8] = b'infimum\x00'

    sup_hs = (1 << 3) | REC_STATUS_SUPREMUM
    page[SUP - 5] = 0x10
    struct.pack_into('>H', page, SUP - 4, sup_hs)
    struct.pack_into('>H', page, SUP - 2, 0)
    page[SUP:SUP + 8] = b'supremum'

    # User records
    built = []
    for fields, deleted in records_data:
        prefix, data = build_compact_record(fields, deleted=deleted, heap_no=2 + len(built))
        built.append((prefix, data))

    origins = []
    cur = SUP + 8
    for prefix, _ in built:
        origins.append(cur + len(prefix))
        cur = origins[-1] + len(built[len(origins) - 1][1])

    if origins:
        struct.pack_into('>H', page, INF - 2, (origins[0] - INF) & 0xFFFF)

    for i, ((prefix, data), origin) in enumerate(zip(built, origins)):
        nxt = origins[i + 1] - origin if i + 1 < len(origins) else SUP - origin
        pfx = bytearray(prefix)
        struct.pack_into('>H', pfx, len(pfx) - 2, nxt & 0xFFFF)
        start = origin - len(pfx)
        page[start:start + len(pfx)] = pfx
        page[origin:origin + len(data)] = data

    struct.pack_into('>H', page, ph + PAGE_HEAP_TOP, cur)
    struct.pack_into('>H', page, ph + PAGE_N_HEAP, 0x8000 | (2 + len(built)))
    struct.pack_into('>H', page, ph + PAGE_N_RECS, len(built))

    return bytes(page)


# ── 测试 ────────────────────────────────────────────────────────

def run_tests():
    print("=" * 60)
    print("InnoDB Recovery Tool v2 — Unit Tests")
    print("=" * 60)

    # 列定义 (dataclass)
    c_id   = ColumnDef(name='id',   type_str='bigint',      length=8, nullable=False, unsigned=True)
    c_name = ColumnDef(name='name', type_str='varchar(64)',  length=64, nullable=True,  charset='utf8mb4')
    c_age  = ColumnDef(name='age',  type_str='int',          length=4,  nullable=True,  unsigned=False)
    columns = [c_id, c_name, c_age]

    # 构造测试页
    records = [
        ([(c_id, 1), (c_name, 'Alice'), (c_age, 30)], False),
        ([(c_id, 2), (c_name, 'Bob'),   (c_age, 25)], True),    # soft deleted
        ([(c_id, 3), (c_name, None),    (c_age, None)], False),  # NULLs
    ]

    page_data = build_test_page(records)
    assert len(page_data) == UNIV_PAGE_SIZE

    # ── Page Header parse ──
    hdr = parse_page_header(page_data)
    assert hdr is not None
    assert hdr.page_type == PAGE_TYPE_INDEX
    assert hdr.is_compact
    assert hdr.is_leaf
    assert hdr.n_recs == 3
    print(f"\nPage header: index_id={hdr.index_id}, level={hdr.page_level}, "
          f"n_recs={hdr.n_recs}, n_heap={hdr.n_heap}")
    print("PASS: parse_page_header")

    # ── Chain scan (include deleted) ──
    parser = RecordParser(columns, include_deleted=True)
    rows = parser.parse_compact(page_data, hdr, page_no=0, source_offset=0)
    print(f"\nChain scan ({len(rows)} rows):")
    for r in rows:
        flag = "[DELETED]" if r.deleted else "[ACTIVE] "
        print(f"  {flag} heap_no={r.heap_no} row={r.row}")

    assert len(rows) == 3
    assert rows[0].row['id'] == 1
    assert rows[0].row['name'] == 'Alice'
    assert rows[0].row['age'] == 30
    assert rows[1].deleted == True
    assert rows[2].row['name'] is None
    print("PASS: chain scan (include deleted)")

    # ── Chain scan (exclude deleted) ──
    parser2 = RecordParser(columns, include_deleted=False)
    rows2 = parser2.parse_compact(page_data, hdr, 0, 0)
    assert len(rows2) == 2
    print("PASS: chain scan (exclude deleted)")

    # ── Brute force ──
    bf_rows = parser.brute_force(page_data, hdr, 0, 0)
    assert len(bf_rows) >= 3
    print(f"PASS: brute force scan ({len(bf_rows)} rows)")

    # ── RecoveryScanner + FilePageSource ──
    with tempfile.NamedTemporaryFile(suffix='.ibd', delete=False) as f:
        f.write(b'\x00' * UNIV_PAGE_SIZE)  # page 0 (FSP)
        f.write(page_data)                  # page 1 (INDEX)
        f.write(b'\x00' * UNIV_PAGE_SIZE)  # page 2
        f.write(page_data)                  # page 3 (dup INDEX)
        tmp_path = f.name

    schema = TableSchema(table='test_table', row_format='COMPACT', columns=columns)
    source = FilePageSource(tmp_path)
    scanner = RecoveryScanner(source, schema, workers=1, brute_force=False,
                              include_deleted=True)

    result = scanner.recover()
    print(f"\nRecoveryScanner: {len(result.rows)} rows (deduped), "
          f"pages_matched={result.pages_matched}, elapsed={result.elapsed:.2f}s")
    assert len(result.rows) == 3, f"Expected 3, got {len(result.rows)}"
    print("PASS: RecoveryScanner + dedup")

    source.close()
    os.unlink(tmp_path)

    # ── OutputWriter ──
    writer = OutputWriter(schema)
    sql = writer.to_sql(result.rows)
    assert 'INSERT INTO' in sql
    assert '-- [DELETED]' in sql
    print("PASS: SQL output")

    csv_out = writer.to_csv(result.rows)
    lines = [l for l in csv_out.strip().split('\n') if l]
    assert len(lines) == 4  # header + 3
    print("PASS: CSV output")

    j = json.loads(writer.to_json(result.rows))
    assert len(j) == 3
    print("PASS: JSON output")

    # ── SDI extraction ──
    print()
    sdi_json = {
        "sdi_version": 80019,
        "dd_object_type": "Table",
        "dd_object": {
            "name": "test_table",
            "schema_ref": "test_db",
            "row_format": 2,
            "columns": [
                {"name": "id",    "type": 8,  "is_nullable": False, "is_unsigned": True,
                 "char_length": 20, "numeric_precision": 0, "collation_id": 0},
                {"name": "name",  "type": 15, "is_nullable": True, "is_unsigned": False,
                 "char_length": 64, "collation_id": 45},
                {"name": "score", "type": 3,  "is_nullable": True, "is_unsigned": False,
                 "char_length": 11, "numeric_precision": 0, "collation_id": 0},
            ]
        }
    }
    raw_json = json.dumps(sdi_json).encode('utf-8')
    compressed = zlib.compress(raw_json)

    sdi_page = bytearray(UNIV_PAGE_SIZE)
    struct.pack_into('>H', sdi_page, FIL_PAGE_TYPE, PAGE_TYPE_SDI)
    sdi_page[FIL_PAGE_DATA:FIL_PAGE_DATA + len(compressed)] = compressed

    # SDI via MemPageSource
    src = MemPageSource([bytes(sdi_page)], page_offsets=[0])
    schema2 = SDIExtractor.extract_from_source(src, table_filter='test_table')
    assert schema2 is not None
    assert schema2.database == 'test_db'
    assert schema2.table == 'test_table'
    assert schema2.row_format == 'DYNAMIC'
    assert len(schema2.columns) == 3
    assert schema2.columns[0].name == 'id'
    assert schema2.columns[0].unsigned == True
    assert schema2.columns[1].name == 'name'
    assert schema2.columns[1].max_len == 64
    print(f"PASS: SDI extraction: {schema2.full_name} ({schema2.row_format}), "
          f"{len(schema2.columns)} columns")

    # SDI table filter
    schema3 = SDIExtractor.extract_from_source(src, table_filter='nonexistent')
    assert schema3 is None
    print("PASS: SDI table filter")

    # ── SDI via FilePageSource ──
    with tempfile.NamedTemporaryFile(suffix='.ibd', delete=False) as f:
        f.write(bytes(sdi_page))
        tmp_sdi = f.name

    src2 = FilePageSource(tmp_sdi)
    schema4 = SDIExtractor.extract_from_source(src2, table_filter='test_table')
    assert schema4 is not None
    assert schema4.database == 'test_db'
    assert schema4.table == 'test_table'
    print(f"PASS: SDI from file: {schema4.full_name}")
    src2.close()
    os.unlink(tmp_sdi)

    # ── Dedup test ──
    r1 = RecoveredRow(row={'id': 1, 'name': 'A'}, page_no=0, heap_no=2)
    r2 = RecoveredRow(row={'id': 2, 'name': 'B'}, page_no=0, heap_no=3)
    r3 = RecoveredRow(row={'id': 1, 'name': 'A'}, page_no=0, heap_no=2)  # same key
    keys = {r1.row_key(), r2.row_key(), r3.row_key()}
    assert len(keys) == 2  # r1 and r3 have same key
    print("PASS: dedup key (page_no + heap_no)")

    # ── PageRef ──
    ref = PageRef(offset=0, page_no=1, page_type=PAGE_TYPE_INDEX)
    assert ref.is_index
    assert ref.type_name == 'INDEX'
    print("PASS: PageRef")

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)


if __name__ == '__main__':
    run_tests()
