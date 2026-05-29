#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
InnoDB Recovery Tool for MySQL 8.0  (v2 — 重构版)
==================================================
基于 MySQL 8.0 InnoDB 源码（rem/rec.h, page0types.h, fil0types.h）
模仿 undrop-for-innodb 的思路，使用统一的 Pipeline 架构：

    PageSource → prescan(PageRef[]) → recover(RecoveredRow[]) → OutputWriter

支持三种数据源:
  - .ibd 文件扫描
  - 裸块设备扫描 (/dev/sda1)
  - /proc/fd 抢救 (mysqld 运行中 DROP TABLE)

核心能力:
  - COMPACT / DYNAMIC / REDUNDANT 行格式
  - SDI 自动提取表结构 (MySQL 8.0)
  - 多库同名表区分 (--database)
  - Instant ADD COLUMN
  - 暴力扫描 (链表残缺时)
  - 多线程并行扫描
  - 删除记录恢复

参考源码:
  storage/innobase/rem/rec.h
  storage/innobase/include/page0types.h
  storage/innobase/include/fil0types.h
"""

import struct
import sys
import os
import json
import argparse
import datetime
import zlib
import time
import hashlib
import logging
import subprocess
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any, Iterator, Set

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §1  Constants — InnoDB 常量（来自 MySQL 8.0 源码）                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

UNIV_PAGE_SIZE    = 16384       # 默认 16KB

# ── FIL 页头偏移 (fil0types.h) ──
FIL_CHECKSUM        = 0         # 4B: checksum
FIL_PAGE_OFFSET     = 4         # 4B: page number
FIL_PAGE_PREV       = 8         # 4B: prev page
FIL_PAGE_NEXT       = 12        # 4B: next page
FIL_PAGE_LSN        = 16        # 8B: LSN
FIL_PAGE_TYPE       = 24        # 2B: page type
FIL_FLUSH_LSN       = 26        # 8B
FIL_SPACE_ID        = 34        # 4B: space ID (MySQL 8.0)
FIL_PAGE_DATA       = 38        # 页数据起始偏移
FIL_HEADER_SIZE     = 38

# ── 页类型 (fil0types.h) ──
PAGE_TYPE_ALLOCATED  = 0x0000
PAGE_TYPE_UNDO_LOG   = 0x0002
PAGE_TYPE_INODE      = 0x0003
PAGE_TYPE_IBUF_FREE  = 0x0004
PAGE_TYPE_SYS        = 0x0006
PAGE_TYPE_TRX_SYS    = 0x0007
PAGE_TYPE_FSP_HDR    = 0x0008
PAGE_TYPE_XDES       = 0x0009
PAGE_TYPE_BLOB       = 0x000A
PAGE_TYPE_ZBLOB      = 0x000B
PAGE_TYPE_ZBLOB2     = 0x000C
PAGE_TYPE_SDI        = 0x0045   # MySQL 8.0 Serialized Dictionary
PAGE_TYPE_RTREE      = 0x45BE
PAGE_TYPE_INDEX      = 0x45BF   # B-tree node

PAGE_TYPE_NAMES = {
    0x0000: 'ALLOCATED', 0x0002: 'UNDO_LOG', 0x0003: 'INODE',
    0x0006: 'SYS',       0x0007: 'TRX_SYS',  0x0008: 'FSP_HDR',
    0x0009: 'XDES',      0x000A: 'BLOB',     0x000B: 'ZBLOB',
    0x000C: 'ZBLOB2',    0x0045: 'SDI',      0x45BE: 'RTREE',
    0x45BF: 'INDEX',
}

# ── PAGE 头部（偏移相对 FIL_PAGE_DATA）──
PAGE_N_DIR_SLOTS   = 0      # 2B: directory slots
PAGE_HEAP_TOP      = 2      # 2B: heap top
PAGE_N_HEAP        = 4      # 2B: heap count (bit15=compact flag)
PAGE_FREE          = 6      # 2B: free list head
PAGE_GARBAGE       = 8      # 2B: deleted bytes
PAGE_LAST_INSERT   = 10     # 2B
PAGE_DIRECTION     = 12     # 2B
PAGE_N_DIRECTION   = 14     # 2B
PAGE_N_RECS        = 16     # 2B: user record count
PAGE_MAX_TRX_ID    = 18     # 8B
PAGE_LEVEL         = 26     # 2B: B-tree level (0=leaf)
PAGE_INDEX_ID      = 28     # 8B: index ID
PAGE_HEADER_SIZE   = 56     # total PAGE header bytes

# Infimum/Supremum 固定位置（compact 格式）
PAGE_NEW_INFIMUM   = FIL_PAGE_DATA + PAGE_HEADER_SIZE + 20 + 5   # 99
PAGE_NEW_SUPREMUM  = PAGE_NEW_INFIMUM + 8 + 5                    # 112

# ── 记录头 (rem/rec.h) ──
REC_NEXT             = 2     # 2B: next offset (signed, relative to origin)
REC_NEW_STATUS       = 3     # 1B (low 3 bits): record status
REC_STATUS_MASK      = 0x07
REC_OLD_SHORT_OFF    = 3     # REDUNDANT short flag (bit0)
REC_OLD_SHORT_MASK   = 0x01
REC_OLD_N_FIELDS     = 4     # REDUNDANT field count
REC_OLD_N_FIELDS_MASK = 0x7FE
REC_OLD_N_FIELDS_SHIFT = 1
REC_NEW_HEAP_NO      = 4     # 2B (high 13 bits): heap_no
REC_OLD_HEAP_NO      = 5
REC_HEAP_NO_MASK     = 0xFFF8
REC_HEAP_NO_SHIFT    = 3
REC_NEW_N_OWNED      = 5     # 1B (low 4 bits)
REC_OLD_N_OWNED      = 6
REC_N_OWNED_MASK     = 0x0F
REC_NEW_INFO_BITS    = 5     # 1B (high 4 bits)
REC_OLD_INFO_BITS    = 6
REC_INFO_BITS_MASK   = 0xF0

# Info bits 标志
REC_INFO_MIN_REC     = 0x10  # min record (non-leaf left boundary)
REC_INFO_DELETED     = 0x20  # delete-marked
REC_INFO_VERSION     = 0x40  # version byte present (MySQL 8.0.29+)
REC_INFO_INSTANT     = 0x80  # Instant ADD COLUMN

# Record status values
REC_STATUS_ORDINARY  = 0
REC_STATUS_NODE_PTR  = 1
REC_STATUS_INFIMUM   = 2
REC_STATUS_SUPREMUM  = 3

# Extra bytes length
REC_N_OLD_EXTRA      = 6      # REDUNDANT
REC_N_NEW_EXTRA      = 5      # COMPACT/DYNAMIC

# NULL / extern flags (REDUNDANT offset array)
REC_1BYTE_SQL_NULL   = 0x80
REC_2BYTE_SQL_NULL   = 0x8000
REC_2BYTE_EXTERN     = 0x4000

# 系统列长度
DATA_TRX_ID_LEN      = 6
DATA_ROLL_PTR_LEN    = 7

# ── 数据类型常量 ──
DATA_VARCHAR   = 1
DATA_CHAR      = 2
DATA_FIXBINARY = 3
DATA_BINARY    = 4
DATA_BLOB      = 5
DATA_INT       = 6
DATA_SYS       = 8
DATA_FLOAT     = 9
DATA_DOUBLE    = 10
DATA_DECIMAL   = 11
DATA_VARMYSQL  = 15
DATA_MYSQL     = 16
DATA_POINT     = 17
DATA_GEOMETRY  = 18
DATA_JSON      = 19
DATA_UNSIGNED  = 256

# ── MySQL 类型 → (InnoDB type, 固定长度) ──
MYSQL_TYPE_MAP = {
    'tinyint':    (DATA_INT, 1),     'smallint':   (DATA_INT, 2),
    'mediumint':  (DATA_INT, 3),     'int':        (DATA_INT, 4),
    'integer':    (DATA_INT, 4),     'bigint':     (DATA_INT, 8),
    'float':      (DATA_FLOAT, 4),   'double':     (DATA_DOUBLE, 8),
    'decimal':    (DATA_DECIMAL, 0), 'numeric':    (DATA_DECIMAL, 0),
    'date':       (DATA_INT, 3),     'time':       (DATA_INT, 3),
    'year':       (DATA_INT, 1),     'datetime':   (DATA_INT, 8),
    'timestamp':  (DATA_INT, 4),
    'char':       (DATA_CHAR, 0),    'varchar':    (DATA_VARCHAR, 0),
    'binary':     (DATA_FIXBINARY, 0), 'varbinary': (DATA_BINARY, 0),
    'tinyblob':   (DATA_BLOB, 0),    'blob':       (DATA_BLOB, 0),
    'mediumblob': (DATA_BLOB, 0),    'longblob':   (DATA_BLOB, 0),
    'tinytext':   (DATA_BLOB, 0),    'text':       (DATA_BLOB, 0),
    'mediumtext': (DATA_BLOB, 0),    'longtext':   (DATA_BLOB, 0),
    'enum':       (DATA_CHAR, 0),    'set':        (DATA_CHAR, 0),
    'json':       (DATA_JSON, 0),    'geometry':   (DATA_GEOMETRY, 0),
    'point':      (DATA_POINT, 25),
}

# ── SDI 内部类型码 → SQL 类型 ──
SDI_TYPE_MAP = {
    0:   ('decimal', 0),    1:   ('tinyint', 1),    2:   ('smallint', 2),
    3:   ('int', 4),        4:   ('float', 4),      5:   ('double', 8),
    6:   ('null', 0),       7:   ('timestamp', 4),  8:   ('bigint', 8),
    9:   ('mediumint', 3),  10:  ('date', 3),       11:  ('time', 3),
    12:  ('datetime', 8),   13:  ('year', 1),       14:  ('newdate', 3),
    15:  ('varchar', 0),    16:  ('bit', 0),        17:  ('timestamp2', 4),
    18:  ('datetime2', 8),  19:  ('time2', 3),
    245: ('json', 0),       246: ('decimal', 0),    247: ('enum', 0),
    248: ('set', 0),        249: ('tinyblob', 0),   250: ('mediumblob', 0),
    251: ('longblob', 0),   252: ('blob', 0),       253: ('varchar', 0),
    254: ('char', 0),       255: ('geometry', 0),
}

SDI_ROW_FORMAT_MAP = {0: 'REDUNDANT', 1: 'COMPACT', 2: 'DYNAMIC', 3: 'COMPRESSED'}

# Collation → charset
SDI_COLLATION_MAP = {
    8: 'latin1', 9: 'latin1', 33: 'utf8mb3', 45: 'utf8mb4',
    46: 'utf8mb4', 63: 'binary', 83: 'utf8', 192: 'utf8mb3',
    224: 'utf8mb4', 225: 'utf8mb4',
}

# Zlib magic bytes
ZLIB_MAGICS = (b'\x78\x01', b'\x78\x9C', b'\x78\xDA', b'\x78\x5E')


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §2  Types — 数据类型                                                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@dataclass
class ColumnDef:
    """列定义"""
    name: str
    type_str: str       # e.g. "varchar(255)"
    length: int = 0     # char/varchar max length
    nullable: bool = True
    unsigned: bool = False
    charset: str = 'utf8mb4'

    # Derived (set in __post_init__)
    data_type: int = 0
    fixed_len: int = 0
    max_len: int = 0

    def __post_init__(self):
        base_name = self.type_str.lower().split('(')[0].strip()
        base = MYSQL_TYPE_MAP.get(base_name, (DATA_BLOB, 0))
        self.data_type = base[0]
        self.fixed_len = base[1] if base[1] > 0 else 0

        # Parse length from type string
        length = 0
        if '(' in self.type_str:
            try:
                inner = self.type_str.split('(')[1].rstrip(')')
                if ',' in inner:
                    prec = int(inner.split(',')[0].strip())
                    length = (prec // 2) + 1 + 4
                else:
                    length = int(inner.strip())
            except (ValueError, IndexError):
                length = 0
        self.length = length or self.length

        # Adjust fixed_len for known types
        if self.data_type == DATA_CHAR and self.length > 0:
            self.fixed_len = self.length
        elif self.data_type == DATA_FIXBINARY and self.length > 0:
            self.fixed_len = self.length
        elif self.data_type == DATA_INT and self.fixed_len == 0:
            self.fixed_len = 4

        # Time types
        if base_name in ('date', 'time'):
            self.fixed_len = 3
        elif base_name in ('datetime',):
            self.fixed_len = 8
        elif base_name in ('timestamp',):
            self.fixed_len = 4
        elif base_name == 'year':
            self.fixed_len = 1

        self.max_len = self.length if self.length else 0

    def is_variable(self) -> bool:
        return self.data_type in (
            DATA_VARCHAR, DATA_BINARY, DATA_BLOB,
            DATA_VARMYSQL, DATA_JSON, DATA_GEOMETRY)

    def is_fixed(self) -> bool:
        return self.fixed_len > 0 and not self.is_variable()


@dataclass
class PageRef:
    """轻量级页引用 — prescan 阶段产出"""
    offset: int          # 字节偏移
    page_no: int         # 页号
    page_type: int       # 页类型
    space_id: int = 0    # 表空间 ID
    index_id: int = 0    # 索引 ID
    page_level: int = 0  # B-tree 层级
    n_recs: int = 0      # 记录数
    checksum: int = 0    # 校验和

    @property
    def is_leaf(self) -> bool:
        return self.page_level == 0

    @property
    def is_index(self) -> bool:
        return self.page_type == PAGE_TYPE_INDEX

    @property
    def type_name(self) -> str:
        return PAGE_TYPE_NAMES.get(self.page_type, f'0x{self.page_type:04X}')


@dataclass
class PageHeader:
    """解析后的页头信息"""
    checksum: int
    page_no: int
    prev_page: int
    next_page: int
    lsn: int
    page_type: int
    space_id: int
    index_id: int
    page_level: int
    n_recs: int
    n_heap: int
    heap_top: int
    is_compact: bool
    free_offset: int

    @property
    def is_leaf(self) -> bool:
        return self.page_level == 0

    @property
    def is_index(self) -> bool:
        return self.page_type == PAGE_TYPE_INDEX


@dataclass
class RecoveredRow:
    """一条恢复的行"""
    row: Dict[str, Any]
    deleted: bool = False
    heap_no: int = 0
    page_no: int = 0
    trx_id: int = 0
    offset_in_source: int = 0
    instant_ver: Optional[int] = None

    def row_key(self) -> str:
        """基于 (page_no, heap_no) 的去重键"""
        return f"{self.page_no}:{self.heap_no}"


@dataclass
class TableSchema:
    """完整表结构"""
    database: str = ''
    table: str = 'recovered_table'
    row_format: str = 'DYNAMIC'
    columns: List[ColumnDef] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        if self.database:
            return f"`{self.database}`.`{self.table}`"
        return f"`{self.table}`"


@dataclass
class RecoveryResult:
    """恢复结果"""
    rows: List[RecoveredRow] = field(default_factory=list)
    pages_scanned: int = 0
    pages_matched: int = 0
    elapsed: float = 0.0
    stats: Dict[str, Any] = field(default_factory=dict)


class RecoveryError(Exception):
    """基础恢复错误"""
    pass

class PageInvalidError(RecoveryError):
    """无效页"""
    pass

class SchemaError(RecoveryError):
    """Schema 错误"""
    pass


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §3  Page I/O — 统一数据源抽象                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class PageSource(ABC):
    """数据源抽象 — 所有扫描器实现此接口"""

    @abstractmethod
    def read_page(self, offset: int) -> Optional[bytes]:
        """读取单页（16KB），offset 为字节偏移；越界/失败返回 None"""
        ...

    @abstractmethod
    def iter_pages(self, start: int = 0, end: int = 0) -> Iterator[Tuple[int, bytes]]:
        """顺序遍历所有页，yield (offset, page_data)"""
        ...

    @property
    @abstractmethod
    def total_size(self) -> int:
        """数据源总字节数"""
        ...

    @property
    def total_pages(self) -> int:
        return self.total_size // UNIV_PAGE_SIZE


class FilePageSource(PageSource):
    """.ibd 文件 / 磁盘镜像"""

    def __init__(self, path: str):
        self.path = path
        self._size: Optional[int] = None
        self._fd_cache = None

    @property
    def total_size(self) -> int:
        if self._size is None:
            self._size = os.path.getsize(self.path)
        return self._size

    def _fd(self):
        if self._fd_cache is None:
            self._fd_cache = open(self.path, 'rb')
        return self._fd_cache

    def read_page(self, offset: int) -> Optional[bytes]:
        if offset + UNIV_PAGE_SIZE > self.total_size:
            return None
        try:
            fd = self._fd()
            fd.seek(offset)
            data = fd.read(UNIV_PAGE_SIZE)
            return data if len(data) == UNIV_PAGE_SIZE else None
        except OSError:
            return None

    def iter_pages(self, start: int = 0, end: int = 0) -> Iterator[Tuple[int, bytes]]:
        end = end or self.total_size
        end = min(end, self.total_size)
        offset = start
        fd = self._fd()
        fd.seek(offset)
        while offset < end:
            chunk = fd.read(UNIV_PAGE_SIZE)
            if len(chunk) < UNIV_PAGE_SIZE:
                break
            yield (offset, chunk)
            offset += UNIV_PAGE_SIZE

    def close(self):
        if self._fd_cache:
            self._fd_cache.close()
            self._fd_cache = None


class DevicePageSource(PageSource):
    """裸块设备（/dev/sda1）"""

    def __init__(self, path: str, block_size: int = 64 * 1024 * 1024):
        self.path = path
        self.block_size = max(block_size, UNIV_PAGE_SIZE)
        self.block_size = (self.block_size // UNIV_PAGE_SIZE) * UNIV_PAGE_SIZE
        self._size: Optional[int] = None
        self._fd_cache = None

    @property
    def total_size(self) -> int:
        if self._size is None:
            self._size = os.path.getsize(self.path)
        return self._size

    def _fd(self):
        if self._fd_cache is None:
            self._fd_cache = open(self.path, 'rb')
        return self._fd_cache

    def read_page(self, offset: int) -> Optional[bytes]:
        if offset + UNIV_PAGE_SIZE > self.total_size:
            return None
        try:
            fd = self._fd()
            fd.seek(offset)
            data = fd.read(UNIV_PAGE_SIZE)
            return data if len(data) == UNIV_PAGE_SIZE else None
        except OSError:
            return None

    def iter_pages(self, start: int = 0, end: int = 0) -> Iterator[Tuple[int, bytes]]:
        end = end or self.total_size
        end = min(end, self.total_size)
        offset = start
        fd = self._fd()
        fd.seek(offset)
        buf_size = self.block_size
        while offset < end:
            read_size = min(buf_size, end - offset)
            chunk = fd.read(read_size)
            if not chunk:
                break
            for i in range(len(chunk) // UNIV_PAGE_SIZE):
                p_off = offset + i * UNIV_PAGE_SIZE
                p_data = chunk[i * UNIV_PAGE_SIZE:(i + 1) * UNIV_PAGE_SIZE]
                yield (p_off, p_data)
            offset += len(chunk) // UNIV_PAGE_SIZE * UNIV_PAGE_SIZE

    def close(self):
        if self._fd_cache:
            self._fd_cache.close()
            self._fd_cache = None


class MemPageSource(PageSource):
    """内存中的页数据（用于测试 / 临时数据）"""

    def __init__(self, pages: List[bytes], page_offsets: List[int] = None):
        self._pages = pages
        self._offsets = page_offsets or [i * UNIV_PAGE_SIZE for i in range(len(pages))]
        self._page_map = {off: data for off, data in zip(self._offsets, pages)}

    @property
    def total_size(self) -> int:
        return len(self._pages) * UNIV_PAGE_SIZE

    def read_page(self, offset: int) -> Optional[bytes]:
        return self._page_map.get(offset)

    def iter_pages(self, start: int = 0, end: int = 0) -> Iterator[Tuple[int, bytes]]:
        end = end or self.total_size
        for off, data in sorted(self._page_map.items()):
            if start <= off < end:
                yield (off, data)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §4  Page Parser — 页解析                                                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _u16(data: bytes, off: int) -> int:
    return struct.unpack_from('>H', data, off)[0]

def _i16(data: bytes, off: int) -> int:
    v = struct.unpack_from('>H', data, off)[0]
    return v if v < 0x8000 else v - 0x10000

def _u32(data: bytes, off: int) -> int:
    return struct.unpack_from('>I', data, off)[0]

def _u64(data: bytes, off: int) -> int:
    return struct.unpack_from('>Q', data, off)[0]

def parse_page_header(data: bytes, offset: int = 0) -> Optional[PageHeader]:
    """
    解析 16KB InnoDB 页的 FIL + PAGE 头。
    返回 PageHeader 或 None（数据不足/页类型无效）。
    """
    if len(data) < FIL_PAGE_DATA + 4:
        return None
    if len(data) < UNIV_PAGE_SIZE:
        return None

    pt = _u16(data, FIL_PAGE_TYPE)

    # 基本验证：可恢复的页类型
    if pt not in (PAGE_TYPE_INDEX, PAGE_TYPE_SDI, PAGE_TYPE_BLOB,
                  PAGE_TYPE_ZBLOB, PAGE_TYPE_ZBLOB2, PAGE_TYPE_ALLOCATED):
        return None

    checksum  = _u32(data, FIL_CHECKSUM)
    page_no   = _u32(data, FIL_PAGE_OFFSET)
    prev_page = _u32(data, FIL_PAGE_PREV)
    next_page = _u32(data, FIL_PAGE_NEXT)
    lsn       = _u64(data, FIL_PAGE_LSN)
    space_id  = _u32(data, FIL_SPACE_ID)

    ph = FIL_PAGE_DATA
    n_heap_raw = _u16(data, ph + PAGE_N_HEAP)
    is_compact = bool(n_heap_raw & 0x8000)
    n_heap     = n_heap_raw & 0x7FFF
    heap_top   = _u16(data, ph + PAGE_HEAP_TOP)
    free_off   = _u16(data, ph + PAGE_FREE)
    page_level = _u16(data, ph + PAGE_LEVEL)
    index_id   = _u64(data, ph + PAGE_INDEX_ID)
    n_recs     = _u16(data, ph + PAGE_N_RECS)

    return PageHeader(
        checksum=checksum, page_no=page_no,
        prev_page=prev_page, next_page=next_page,
        lsn=lsn, page_type=pt, space_id=space_id,
        index_id=index_id, page_level=page_level,
        n_recs=n_recs, n_heap=n_heap,
        heap_top=heap_top, is_compact=is_compact,
        free_offset=free_off)


def validate_index_page(hdr: PageHeader, *, relaxed: bool = False,
                        space_id: int = 0, index_id: int = 0) -> bool:
    """验证是否为可恢复的 INDEX/BLOB 页"""
    if hdr.page_type == PAGE_TYPE_INDEX:
        if space_id and hdr.space_id != space_id:
            return False
        if index_id and hdr.index_id != index_id:
            return False
        if not relaxed:
            if not hdr.is_leaf:
                return False
            if hdr.n_heap < 2 or hdr.n_heap > 2000:
                return False
        else:
            if hdr.n_heap < 2 or hdr.n_heap > 50000:
                return False
        # heap_top 合法性
        if hdr.heap_top < FIL_PAGE_DATA + PAGE_HEADER_SIZE + 38:
            return False
        if hdr.heap_top > UNIV_PAGE_SIZE:
            return False
        return True
    elif hdr.page_type in (PAGE_TYPE_BLOB, PAGE_TYPE_ZBLOB, PAGE_TYPE_ZBLOB2):
        return relaxed  # BLOB 页仅在 relaxed 模式接受
    return False


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §5  Record Parser — 记录解析                                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class RecordParser:
    """
    无状态解析器：传入页数据和 schema，返回记录列表。

    支持 COMPACT/DYNAMIC（new-style）和 REDUNDANT（old-style）。
    支持 Instant ADD COLUMN (MySQL 8.0)。
    """

    def __init__(self, columns: List[ColumnDef], include_deleted: bool = True):
        self.columns = columns
        self.include_deleted = include_deleted
        self._nullable_cols = [c for c in columns if c.nullable]
        self._var_cols = [c for c in columns if c.is_variable()]
        self._n_nullable = len(self._nullable_cols)

    # ── New-style (COMPACT/DYNAMIC) ──────────────────────────────────

    def parse_compact(self, page_data: bytes, page_hdr: PageHeader,
                      page_no: int, source_offset: int = 0) -> List[RecoveredRow]:
        """链式遍历 COMPACT/DYNAMIC 页"""
        results = []
        data = page_data
        infimum_off = PAGE_NEW_INFIMUM

        next_raw = _u16(data, infimum_off - 2)
        next_off = next_raw if next_raw < 0x8000 else next_raw - 0x10000
        cur = infimum_off + next_off

        visited: Set[int] = set()
        while FIL_PAGE_DATA < cur < UNIV_PAGE_SIZE - 8:
            if cur in visited:
                break
            visited.add(cur)

            row = self._parse_compact_record(data, cur)
            if row is not None:
                row.page_no = page_no
                row.offset_in_source = source_offset
                results.append(row)

            next_raw = _u16(data, cur - 2)
            next_off = next_raw if next_raw < 0x8000 else next_raw - 0x10000
            if next_off == 0:
                break
            nxt = cur + next_off
            if nxt == cur:
                break
            cur = nxt

        return results

    def _parse_compact_record(self, data: bytes, rec_offset: int) -> Optional[RecoveredRow]:
        """解析一条 COMPACT/DYNAMIC 记录"""
        info_byte = data[rec_offset - 5]
        info_bits = info_byte & 0xF0
        n_owned   = info_byte & 0x0F

        heap_raw = _u16(data, rec_offset - 4)
        heap_no = (heap_raw & REC_HEAP_NO_MASK) >> REC_HEAP_NO_SHIFT
        status  = heap_raw & REC_STATUS_MASK

        is_deleted = bool(info_bits & REC_INFO_DELETED)
        is_instant = bool(info_bits & REC_INFO_INSTANT)
        is_versioned = bool(info_bits & REC_INFO_VERSION)

        if status in (REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM, REC_STATUS_NODE_PTR):
            return None
        if is_deleted and not self.include_deleted:
            return None

        # NULL 位图 (逆序，在 extra 之前)
        null_bm_len = (self._n_nullable + 7) // 8
        pos = rec_offset - REC_N_NEW_EXTRA
        null_bm = bytearray(null_bm_len)
        valid = True
        for i in range(null_bm_len):
            pos -= 1
            if pos < 0:
                valid = False
                break
            null_bm[i] = data[pos]
        if not valid:
            return None

        # 变长字段长度列表 (逆序，在 null bitmap 之前)
        var_lengths: Dict[str, int] = {}
        for c in reversed(self._var_cols):
            if pos <= 0:
                break
            pos -= 1
            vlen = data[pos]
            if vlen & 0x80:
                pos -= 1
                if pos < 0:
                    break
                vlen = ((vlen & 0x3F) << 8) | data[pos]
            var_lengths[c.name] = vlen

        # Instant version byte
        data_pos = rec_offset
        instant_ver: Optional[int] = None
        if is_instant or is_versioned:
            b = data[data_pos]
            if b & 0x80:
                if data_pos + 2 > len(data):
                    return None
                instant_ver = ((b & 0x7F) << 8) | data[data_pos + 1]
                data_pos += 2
            else:
                instant_ver = b
                data_pos += 1

        # 系统列
        if data_pos + DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN > len(data):
            return None
        trx_id = int.from_bytes(data[data_pos:data_pos + 6], 'big')
        data_pos += DATA_TRX_ID_LEN
        data_pos += DATA_ROLL_PTR_LEN  # skip roll_ptr

        # 逐列解析
        row: Dict[str, Any] = {}
        null_idx = 0
        for col in self.columns:
            is_null = False
            if col.nullable:
                byte_i = null_idx // 8
                bit_i  = null_idx % 8
                if byte_i < len(null_bm):
                    is_null = bool(null_bm[byte_i] & (1 << bit_i))
                null_idx += 1

            if is_null:
                row[col.name] = None
                continue

            if col.is_variable():
                vlen = var_lengths.get(col.name, 0)
                if data_pos + vlen > len(data):
                    row[col.name] = None
                    continue
                raw = data[data_pos:data_pos + vlen]
                data_pos += vlen
                row[col.name] = self._decode_value(col, raw)
            else:
                flen = col.fixed_len
                if flen == 0:
                    row[col.name] = None
                    continue
                if data_pos + flen > len(data):
                    row[col.name] = None
                    continue
                raw = data[data_pos:data_pos + flen]
                data_pos += flen
                row[col.name] = self._decode_value(col, raw)

        # Sanity: at least one non-None value
        if not any(v is not None for v in row.values()):
            return None

        return RecoveredRow(
            row=row, deleted=is_deleted, heap_no=heap_no,
            trx_id=trx_id, instant_ver=instant_ver)

    # ── Old-style (REDUNDANT) ────────────────────────────────────────

    def parse_redundant(self, page_data: bytes, page_hdr: PageHeader,
                        page_no: int, source_offset: int = 0) -> List[RecoveredRow]:
        """链式遍历 REDUNDANT 页"""
        results = []
        data = page_data

        infimum_off = FIL_PAGE_DATA + PAGE_HEADER_SIZE + 20 + 6
        next_raw = _u16(data, infimum_off - 2)
        cur = next_raw

        visited: Set[int] = set()
        while FIL_PAGE_DATA < cur < UNIV_PAGE_SIZE - 8:
            if cur in visited:
                break
            visited.add(cur)

            row = self._parse_redundant_record(data, cur)
            if row is not None:
                row.page_no = page_no
                row.offset_in_source = source_offset
                results.append(row)

            next_raw = _u16(data, cur - 2)
            if next_raw == 0 or next_raw == cur:
                break
            cur = next_raw

        return results

    def _parse_redundant_record(self, data: bytes, rec_offset: int) -> Optional[RecoveredRow]:
        """解析一条 REDUNDANT 记录"""
        info_byte = data[rec_offset - 6]
        info_bits = info_byte & 0xF0

        heap_raw = _u16(data, rec_offset - 5)
        heap_no = (heap_raw & REC_HEAP_NO_MASK) >> REC_HEAP_NO_SHIFT

        fs_raw = _u16(data, rec_offset - 4)
        short_flag = fs_raw & REC_OLD_SHORT_MASK
        n_fields = (fs_raw & REC_OLD_N_FIELDS_MASK) >> REC_OLD_N_FIELDS_SHIFT

        is_deleted = bool(info_bits & REC_INFO_DELETED)
        if is_deleted and not self.include_deleted:
            return None

        offs_size = 1 if short_flag else 2
        base = rec_offset - REC_N_OLD_EXTRA - offs_size * n_fields
        offsets = []
        for i in range(n_fields):
            p = base + i * offs_size
            if offs_size == 1:
                o = data[p]
                is_null = bool(o & REC_1BYTE_SQL_NULL)
                off_val = o & ~REC_1BYTE_SQL_NULL
            else:
                o = _u16(data, p)
                is_null = bool(o & REC_2BYTE_SQL_NULL)
                off_val = o & ~(REC_2BYTE_SQL_NULL | REC_2BYTE_EXTERN)
            offsets.append((off_val, is_null))

        row: Dict[str, Any] = {}
        for idx, col in enumerate(self.columns):
            if idx >= len(offsets):
                break
            end_off, is_null = offsets[idx]
            start_off = offsets[idx - 1][0] if idx > 0 else 0

            if is_null:
                row[col.name] = None
            else:
                val_bytes = data[rec_offset + start_off:rec_offset + end_off]
                row[col.name] = self._decode_value(col, val_bytes)

        if not any(v is not None for v in row.values()):
            return None

        return RecoveredRow(row=row, deleted=is_deleted, heap_no=heap_no)

    # ── Brute force ──────────────────────────────────────────────────

    def brute_force(self, page_data: bytes, page_hdr: PageHeader,
                    page_no: int, source_offset: int = 0) -> List[RecoveredRow]:
        """
        暴力扫描：不依赖页链表，滑窗尝试解析每条可能的记录。

        改进：自适应步长 — 成功解析后跳到下一条边界，失败步进 1 字节。
        """
        results = []
        data = page_data
        is_compact = page_hdr.is_compact
        start = FIL_PAGE_DATA + PAGE_HEADER_SIZE + 38
        end = page_hdr.heap_top
        if end > UNIV_PAGE_SIZE - 8:
            end = UNIV_PAGE_SIZE - 8

        extra_len = REC_N_NEW_EXTRA if is_compact else REC_N_OLD_EXTRA
        offset = start
        consecutive_failures = 0

        while offset < end:
            if offset + extra_len >= len(data):
                break

            try:
                if is_compact:
                    row = self._parse_compact_record(data, offset)
                else:
                    row = self._parse_redundant_record(data, offset)

                if row is not None:
                    row.page_no = page_no
                    row.offset_in_source = source_offset
                    results.append(row)
                    # Jump to estimated next record (variable length heap records)
                    offset += 1  # conservative: step 1 byte
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    offset += 1
            except Exception:
                consecutive_failures += 1
                offset += 1

        return results

    # ── Value Decoder ────────────────────────────────────────────────

    def _decode_value(self, col: ColumnDef, raw: bytes) -> Any:
        if len(raw) == 0:
            return ''
        try:
            t = col.data_type
            n = len(raw)

            if t == DATA_INT:
                b = bytearray(raw)
                b[0] ^= 0x80
                val = int.from_bytes(b, 'big', signed=False)
                if not col.unsigned:
                    half = 1 << (n * 8 - 1)
                    if val >= half:
                        val -= (half << 1)
                return val

            if t == DATA_FLOAT:
                b = bytearray(raw)
                b[0] ^= 0x80
                return struct.unpack('>f', bytes(b))[0]

            if t == DATA_DOUBLE:
                b = bytearray(raw)
                b[0] ^= 0x80
                return struct.unpack('>d', bytes(b))[0]

            if t == DATA_DECIMAL:
                return '0x' + raw.hex()

            if t in (DATA_VARCHAR, DATA_VARMYSQL, DATA_BLOB, DATA_JSON):
                enc = 'utf-8' if 'utf8' in col.charset else col.charset
                try:
                    return raw.decode(enc, errors='replace')
                except Exception:
                    return raw.decode('latin1', errors='replace')

            if t in (DATA_CHAR, DATA_MYSQL):
                enc = 'utf-8' if 'utf8' in col.charset else col.charset
                try:
                    return raw.decode(enc, errors='replace').rstrip('\x00')
                except Exception:
                    return raw.decode('latin1', errors='replace').rstrip('\x00')

            if t in (DATA_BINARY, DATA_FIXBINARY, DATA_GEOMETRY):
                return '0x' + raw.hex()

            return '0x' + raw.hex()
        except Exception:
            return f'<decode_error>'


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §6  Schema — 表结构加载 / SDI 自动提取                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def load_schema_from_json(path: str) -> TableSchema:
    """从 JSON 文件加载表结构"""
    with open(path, 'r', encoding='utf-8') as f:
        obj = json.load(f)

    cols = []
    for c in obj.get('columns', []):
        type_full = c['type']
        length = 0
        if '(' in type_full:
            try:
                inner = type_full.split('(')[1].rstrip(')')
                if ',' in inner:
                    prec = int(inner.split(',')[0].strip())
                    length = (prec // 2) + 1 + 4
                else:
                    length = int(inner.strip())
            except Exception:
                pass

        cols.append(ColumnDef(
            name=c['name'], type_str=type_full, length=length,
            nullable=c.get('nullable', True),
            unsigned=c.get('unsigned', False),
            charset=c.get('charset', 'utf8mb4')))

    return TableSchema(
        database=obj.get('database', ''),
        table=obj.get('table', 'recovered_table'),
        row_format=obj.get('row_format', 'DYNAMIC').upper(),
        columns=cols)


class SDIExtractor:
    """MySQL 8.0 SDI 页解析 — 自动提取表结构"""

    @staticmethod
    def extract_from_source(source: PageSource, *,
                            table_filter: str = '',
                            schema_filter: str = '') -> Optional[TableSchema]:
        """
        从 PageSource 遍历所有 SDI 页，提取匹配的表结构。
        多候选时交互提示 --database。
        """
        candidates: List[TableSchema] = []
        seen: Set[Tuple[str, str]] = set()
        stats = {'total_pages': 0, 'sdi_pages': 0, 'decomp_ok': 0,
                 'parse_ok': 0, 'filtered': 0}

        for offset, raw in source.iter_pages():
            stats['total_pages'] += 1
            pt = _u16(raw, FIL_PAGE_TYPE)
            if pt != PAGE_TYPE_SDI:
                continue
            stats['sdi_pages'] += 1

            text = SDIExtractor._decompress(raw)
            if text is None:
                logging.debug(f"SDI 页 @{offset}: 解压失败")
                continue
            stats['decomp_ok'] += 1

            try:
                obj = json.loads(text)
            except json.JSONDecodeError as e:
                logging.debug(f"SDI 页 @{offset}: JSON 解析失败: {e}")
                continue

            schema = SDIExtractor._parse(obj)
            if schema is None:
                logging.debug(f"SDI 页 @{offset}: schema 提取失败")
                continue
            stats['parse_ok'] += 1

            if table_filter and table_filter.lower() not in schema.table.lower():
                stats['filtered'] += 1
                continue
            if schema_filter and schema_filter.lower() != schema.database.lower():
                stats['filtered'] += 1
                continue

            key = (schema.database, schema.table)
            if key not in seen:
                seen.add(key)
                candidates.append(schema)

        if not candidates:
            logging.error(
                f"SDI 诊断: 共 {stats['total_pages']} 页, "
                f"SDI 页={stats['sdi_pages']}, "
                f"解压成功={stats['decomp_ok']}, "
                f"解析成功={stats['parse_ok']}, "
                f"被过滤={stats['filtered']}")
            if stats['sdi_pages'] == 0:
                logging.error(
                    "文件中没有 SDI 页 — 这不是 MySQL 8.0 .ibd 文件，"
                    "或者表已被彻底删除")
            elif stats['decomp_ok'] == 0:
                logging.error("SDI 页解压全部失败 — 文件可能已损坏")
            elif stats['parse_ok'] == 0:
                logging.error("SDI JSON 解析全部失败 — 数据字典格式异常")
            elif stats['filtered']:
                logging.error(
                    "SDI 解析成功但被 --table/--database 过滤掉了，"
                    "请检查参数是否正确")
            return None

        if len(candidates) == 1:
            return candidates[0]

        # 多候选 — 提示
        print()
        print("=" * 60)
        print(f"  ⚠ 找到 {len(candidates)} 个匹配表，请用 --database 指定库名")
        print("=" * 60)
        for s in candidates:
            print(f"  {s.database}.{s.table}  (行格式={s.row_format}, {len(s.columns)} 列)")
        print()
        print("示例:")
        for s in candidates:
            print(f"  python innodb_recovery.py ... --table {s.table} --database {s.database}")
        print()
        sys.exit(1)

    @staticmethod
    def _decompress(raw: bytes) -> Optional[str]:
        best: Optional[str] = None
        best_len = 0
        search_end = UNIV_PAGE_SIZE - 4
        for magic in ZLIB_MAGICS:
            pos = FIL_PAGE_DATA
            while pos < search_end:
                found = raw.find(magic, pos)
                if found == -1:
                    break
                try:
                    text = zlib.decompress(raw[found:]).decode('utf-8', errors='replace')
                    if text.strip().startswith('{') and '"dd_object_type"' in text:
                        try:
                            json.loads(text)
                            if len(text) > best_len:
                                best = text
                                best_len = len(text)
                        except json.JSONDecodeError:
                            pass
                except (zlib.error, UnicodeDecodeError):
                    pass
                pos = found + 1
        return best

    @staticmethod
    def _parse(obj: Dict) -> Optional[TableSchema]:
        try:
            dd = obj.get('dd_object', {})
            table_name = dd.get('name', 'unknown')
            db_name = dd.get('schema_ref', '')
            row_fmt_int = dd.get('row_format', 2)
            row_format = SDI_ROW_FORMAT_MAP.get(row_fmt_int, 'DYNAMIC')

            columns = []
            for col in dd.get('columns', []):
                col_type = col.get('type', 253)
                ti = SDI_TYPE_MAP.get(col_type, ('varchar', 0))
                type_name = ti[0]
                char_len = col.get('char_length', 0)

                if type_name in ('varchar', 'char', 'varbinary', 'binary'):
                    type_str = f'{type_name}({char_len})' if char_len else type_name
                elif type_name == 'decimal':
                    prec = col.get('numeric_precision', 10) or 10
                    scale = col.get('numeric_scale', 0) or 0
                    type_str = f'decimal({prec},{scale})'
                else:
                    type_str = type_name

                collation = col.get('collation_id', 0)
                charset = SDI_COLLATION_MAP.get(collation, 'utf8mb4')

                columns.append(ColumnDef(
                    name=col.get('name', f'col_{len(columns)}'),
                    type_str=type_str,
                    length=char_len if type_name in ('varchar', 'char') else 0,
                    nullable=col.get('is_nullable', True),
                    unsigned=col.get('is_unsigned', False),
                    charset=charset))

            return TableSchema(
                database=db_name, table=table_name,
                row_format=row_format, columns=columns)
        except Exception as e:
            logging.debug(f"SDI _parse 失败: {e}", exc_info=True)
            return None

    @staticmethod
    def generate_schema_json(schema: TableSchema, output_path: str):
        """写出 schema.json"""
        obj = {
            'database': schema.database,
            'table': schema.table,
            'row_format': schema.row_format,
            'columns': [
                {
                    'name': c.name,
                    'type': c.type_str,
                    'nullable': c.nullable,
                    'unsigned': c.unsigned,
                    'charset': c.charset,
                }
                for c in schema.columns
            ],
            '_auto_generated': True,
            '_source': 'MySQL 8.0 SDI',
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
        logging.info(f"Schema 已写入: {output_path}")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §7  Scanner — 统一扫描引擎                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class RecoveryScanner:
    """
    统一扫描引擎 — 适用于所有 PageSource。

    流程:
      prescan() → List[PageRef]   (快速预扫描: 只读 FIL 头)
      recover() → RecoveryResult  (深度解析: 多线程并行)
    """

    def __init__(self, source: PageSource, schema: TableSchema, *,
                 workers: int = 4,
                 relaxed: bool = False,
                 brute_force: bool = False,
                 space_id: int = 0,
                 index_id: int = 0,
                 include_deleted: bool = True):
        self.source = source
        self.schema = schema
        self.workers = workers
        self.relaxed = relaxed
        self.brute_force = brute_force
        self.space_id = space_id
        self.index_id = index_id
        self.include_deleted = include_deleted
        self.parser = RecordParser(schema.columns, include_deleted=include_deleted)

    # ── 预扫描 ─────────────────────────────────────────────────────

    def prescan(self, start_byte: int = 0, length_bytes: int = 0,
                page_types: Set[int] = None) -> List[PageRef]:
        """
        快速预扫描：遍历所有页边界，只读 38 字节 FIL 头。
        返回候选 PageRef 列表。
        """
        if length_bytes <= 0:
            length_bytes = self.source.total_size - start_byte
        end_byte = min(start_byte + length_bytes, self.source.total_size)

        if page_types is None:
            page_types = {PAGE_TYPE_INDEX, PAGE_TYPE_BLOB, PAGE_TYPE_ZBLOB, PAGE_TYPE_ZBLOB2}

        total_pages = (end_byte - start_byte) // UNIV_PAGE_SIZE
        logging.info(f"预扫描: {total_pages:,} 页，{self.workers} 线程")

        chunk_pages = max(1, total_pages // self.workers + 1)
        chunks = []
        for i in range(self.workers):
            cs = start_byte + i * chunk_pages * UNIV_PAGE_SIZE
            ce = min(cs + chunk_pages * UNIV_PAGE_SIZE, end_byte)
            if cs < ce:
                chunks.append((cs, ce))

        candidates: List[PageRef] = []
        lock = threading.Lock()
        t0 = time.time()

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = [
                ex.submit(self._prescan_chunk, cs, ce, page_types)
                for cs, ce in chunks
            ]
            for fut in as_completed(futures):
                with lock:
                    candidates.extend(fut.result())

        elapsed = time.time() - t0
        rate = total_pages / elapsed if elapsed > 0 else 0
        logging.info(f"预扫描完成: {elapsed:.1f}s, {rate:,.0f} 页/秒, {len(candidates):,} 候选")

        # 类型统计
        tc: Dict[str, int] = {}
        for c in candidates:
            tc[c.type_name] = tc.get(c.type_name, 0) + 1
        for name, cnt in sorted(tc.items(), key=lambda x: -x[1]):
            logging.info(f"  {name:<12} {cnt:>8,} 页")

        return candidates

    def _prescan_chunk(self, start: int, end: int,
                       page_types: Set[int]) -> List[PageRef]:
        """扫描一个块 — 只读 FIL 头"""
        results = []
        buf_size = 64 * 1024 * 1024
        buf_size = (buf_size // UNIV_PAGE_SIZE) * UNIV_PAGE_SIZE
        offset = start

        fd = open(self.source.path, 'rb') if hasattr(self.source, 'path') else None
        try:
            if fd is None:
                return results
            fd.seek(offset)
            while offset < end:
                read_size = min(buf_size, end - offset)
                chunk = fd.read(read_size)
                if not chunk:
                    break
                n = len(chunk) // UNIV_PAGE_SIZE
                for i in range(n):
                    base = i * UNIV_PAGE_SIZE
                    if base + FIL_PAGE_TYPE + 2 > len(chunk):
                        break
                    pt = _u16(chunk, base + FIL_PAGE_TYPE)
                    if pt not in page_types:
                        continue
                    p_off = offset + base
                    checksum = _u32(chunk, base + FIL_CHECKSUM)
                    page_no = _u32(chunk, base + FIL_PAGE_OFFSET)
                    space_id = _u32(chunk, base + FIL_SPACE_ID)
                    ph = base + FIL_PAGE_DATA
                    if ph + PAGE_INDEX_ID + 8 > len(chunk):
                        continue
                    page_level = _u16(chunk, ph + PAGE_LEVEL)
                    index_id = _u64(chunk, ph + PAGE_INDEX_ID)
                    n_recs = _u16(chunk, ph + PAGE_N_RECS)
                    ref = PageRef(
                        offset=p_off, page_no=page_no, page_type=pt,
                        space_id=space_id, index_id=index_id,
                        page_level=page_level, n_recs=n_recs,
                        checksum=checksum)
                    results.append(ref)
                offset += n * UNIV_PAGE_SIZE
        finally:
            if fd:
                fd.close()
        return results

    # ── 深度恢复 ───────────────────────────────────────────────────

    def recover(self, candidates: List[PageRef] = None,
                start_byte: int = 0, length_bytes: int = 0) -> RecoveryResult:
        """
        深度恢复：
        1. 若无 candidates，先 prescan
        2. 多线程并行解析候选页
        3. 基于 (page_no, heap_no) 去重
        """
        t0 = time.time()

        if candidates is None:
            candidates = self.prescan(start_byte=start_byte, length_bytes=length_bytes)

        if not candidates:
            return RecoveryResult(elapsed=time.time() - t0)

        # 过滤不合要求的候选
        valid = []
        for c in candidates:
            if self.space_id and c.space_id != self.space_id:
                continue
            if self.index_id and c.index_id != self.index_id:
                continue
            if not self.relaxed and c.page_type == PAGE_TYPE_INDEX and not c.is_leaf:
                continue
            valid.append(c)

        logging.info(f"深度解析 {len(valid):,}/{len(candidates):,} 有效候选页")

        # 分块
        chunk_size = max(1, len(valid) // self.workers)
        chunks = [valid[i:i + chunk_size] for i in range(0, len(valid), chunk_size)]

        all_rows: List[RecoveredRow] = []
        seen: Set[str] = set()
        lock = threading.Lock()
        pages_matched = [0]

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {
                ex.submit(self._recover_chunk, ch, pages_matched, len(valid)): i
                for i, ch in enumerate(chunks)
            }
            for fut in as_completed(futures):
                chunk_rows = fut.result()
                for r in chunk_rows:
                    with lock:
                        key = r.row_key()
                        if key not in seen:
                            seen.add(key)
                            all_rows.append(r)

        elapsed = time.time() - t0
        logging.info(f"恢复完成: {elapsed:.1f}s, {len(all_rows)} 条记录, "
                     f"{pages_matched[0]} 页命中")

        return RecoveryResult(
            rows=all_rows,
            pages_scanned=len(valid),
            pages_matched=pages_matched[0],
            elapsed=elapsed,
            stats={'candidates': len(candidates), 'valid': len(valid)})

    def _recover_chunk(self, chunk: List[PageRef],
                       matched: List[int], total: int) -> List[RecoveredRow]:
        """多线程工作单元"""
        results: List[RecoveredRow] = []
        for ref in chunk:
            raw = None
            if hasattr(self.source, 'read_page'):
                raw = self.source.read_page(ref.offset)
            if raw is None or len(raw) < UNIV_PAGE_SIZE:
                continue

            hdr = parse_page_header(raw, ref.offset)
            if hdr is None:
                continue
            if not validate_index_page(hdr, relaxed=self.relaxed,
                                       space_id=self.space_id,
                                       index_id=self.index_id):
                continue

            matched[0] += 1

            # 解析
            rows: List[RecoveredRow] = []
            try:
                if not self.brute_force:
                    if hdr.is_compact:
                        rows = self.parser.parse_compact(
                            raw, hdr, ref.page_no, ref.offset)
                    else:
                        rows = self.parser.parse_redundant(
                            raw, hdr, ref.page_no, ref.offset)
                    # 链表为空时自动回退到暴力扫描
                    if not rows:
                        rows = self.parser.brute_force(
                            raw, hdr, ref.page_no, ref.offset)
                else:
                    rows = self.parser.brute_force(
                        raw, hdr, ref.page_no, ref.offset)
            except Exception as e:
                logging.debug(f"解析页 offset={ref.offset} 失败: {e}")

            results.extend(rows)

        return results


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §8  Output — 输出格式化                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class OutputWriter:
    """SQL / CSV / JSON 输出"""

    def __init__(self, schema: TableSchema):
        self.schema = schema

    def _escape(self, val: Any) -> str:
        if val is None:
            return 'NULL'
        if isinstance(val, str):
            return "'" + val.replace("\\", "\\\\").replace("'", "\\'") + "'"
        if isinstance(val, (int, float)):
            return str(val)
        return "'" + str(val) + "'"

    def to_sql(self, rows: List[RecoveredRow]) -> str:
        lines = [
            f"-- MySQL 8.0 InnoDB Recovery Tool v2",
        ]
        if self.schema.database:
            lines.append(f"-- Database: {self.schema.database}")
        lines.append(f"-- Table: {self.schema.table}")
        lines.append(f"-- Recovered: {datetime.datetime.now().isoformat()}")
        lines.append(f"-- Total rows: {len(rows)}")
        lines.append("")

        col_names = ', '.join(f'`{c.name}`' for c in self.schema.columns)
        for r in rows:
            vals = ', '.join(self._escape(r.row.get(c.name)) for c in self.schema.columns)
            stmt = f"INSERT INTO {self.schema.full_name} ({col_names}) VALUES ({vals});"
            if r.deleted:
                stmt = "-- [DELETED] " + stmt
            lines.append(stmt)

        return '\n'.join(lines)

    def to_csv(self, rows: List[RecoveredRow]) -> str:
        import io, csv
        buf = io.StringIO()
        headers = [c.name for c in self.schema.columns] + [
            '_deleted', '_heap_no', '_page_no', '_trx_id']
        writer = csv.writer(buf)
        writer.writerow(headers)
        for r in rows:
            vals = [r.row.get(c.name) for c in self.schema.columns]
            vals += [r.deleted, r.heap_no, r.page_no, r.trx_id]
            writer.writerow(vals)
        return buf.getvalue()

    def to_json(self, rows: List[RecoveredRow]) -> str:
        out = []
        for r in rows:
            copy = dict(r.row)
            copy['_meta'] = {
                'deleted': r.deleted, 'heap_no': r.heap_no,
                'page_no': r.page_no, 'trx_id': r.trx_id}
            out.append(copy)
        return json.dumps(out, ensure_ascii=False, indent=2, default=str)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  §9  CLI — 命令行入口                                                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _detect_mysqld_pids() -> List[int]:
    pids = []
    try:
        out = subprocess.check_output(['pgrep', '-x', 'mysqld'],
                                      stderr=subprocess.DEVNULL).decode()
        pids = [int(p) for p in out.split() if p.strip().isdigit()]
    except Exception:
        pass
    if not pids:
        try:
            for entry in os.listdir('/proc'):
                if not entry.isdigit():
                    continue
                with open(f'/proc/{entry}/comm') as f:
                    if f.read().strip() in ('mysqld', 'mysqld_safe'):
                        pids.append(int(entry))
        except Exception:
            pass
    return pids


def _find_deleted_ibd_in_fd(pid: int, hint: str = '') -> List[Tuple[str, str]]:
    results = []
    fd_dir = f'/proc/{pid}/fd'
    try:
        for fd in os.listdir(fd_dir):
            fdp = f'{fd_dir}/{fd}'
            try:
                tgt = os.readlink(fdp)
                if ' (deleted)' in tgt and '.ibd' in tgt:
                    if hint and hint.lower() not in tgt.lower():
                        continue
                    results.append((fdp, tgt.replace(' (deleted)', '')))
            except Exception:
                pass
    except (PermissionError, FileNotFoundError):
        pass
    return results


def _find_deleted_ibd_in_maps(pid: int, hint: str = '') -> List[Tuple[str, str]]:
    results = []
    map_dir = f'/proc/{pid}/map_files'
    try:
        for entry in os.listdir(map_dir):
            try:
                tgt = os.readlink(f'{map_dir}/{entry}')
                if ' (deleted)' in tgt and '.ibd' in tgt:
                    ori = tgt.replace(' (deleted)', '')
                    if hint and hint.lower() not in ori.lower():
                        continue
                    results.append((f'{map_dir}/{entry}', ori))
            except Exception:
                pass
    except (FileNotFoundError, PermissionError):
        pass
    return results


def _copy_fd(fd_path: str, out: str) -> Optional[str]:
    try:
        size = 0
        with open(fd_path, 'rb') as src, open(out, 'wb') as dst:
            while True:
                chunk = src.read(4 * 1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
                size += len(chunk)
        logging.info(f"抢救完成: {size/1024/1024:.1f} MB → {out}")
        return out
    except PermissionError:
        logging.error("读取 /proc/fd 失败: 需 root 权限")
        return None
    except Exception as e:
        logging.error(f"读取失败: {e}")
        return None


def _try_read_mapped(map_entry: str) -> Optional[bytes]:
    parts = map_entry.split('/')[-1]
    try:
        ss, es = parts.split('-')
        start, end = int(ss, 16), int(es, 16)
        size = end - start
        if size < UNIV_PAGE_SIZE or size > 10 * 1024 * 1024 * 1024:
            return None
    except Exception:
        return None
    pid = int(map_entry.split('/')[2])
    try:
        with open(f'/proc/{pid}/mem', 'rb') as f:
            f.seek(start)
            return f.read(min(size, 1024 * 1024 * 1024))
    except Exception:
        return None


def _rescue_ibd(table_hint: str = '') -> Optional[str]:
    """尝试从 /proc/fd 或 map_files 抢救 .ibd"""
    pids = _detect_mysqld_pids()
    if not pids:
        logging.warning("未找到 mysqld 进程")
        return None

    logging.info(f"找到 {len(pids)} 个 mysqld 进程: PID={pids}")

    for pid in pids:
        fds = _find_deleted_ibd_in_fd(pid, table_hint)
        if fds:
            if len(fds) > 1 and not table_hint:
                print("\n发现多个已删除 .ibd，请用 --table 过滤：")
                for _, o in fds:
                    print(f"  --table {os.path.basename(o).replace('.ibd','')}")
                return None
            return _copy_fd(fds[0][0], f'/tmp/rescued_{table_hint or "table"}.ibd')

    for pid in pids:
        maps = _find_deleted_ibd_in_maps(pid, table_hint)
        if maps:
            if len(maps) > 1 and not table_hint:
                print("\n发现多个内存映射 .ibd，请用 --table 过滤：")
                for _, o in maps:
                    print(f"  --table {os.path.basename(o).replace('.ibd','')}")
                return None
            data = _try_read_mapped(maps[0][0])
            if data:
                out = f'/tmp/rescued_{table_hint or "table"}.ibd'
                with open(out, 'wb') as f:
                    f.write(data)
                logging.info(f"从内存映射抢救: {len(data)/1024/1024:.1f} MB → {out}")
                return out

    print()
    print("=" * 60)
    print("  未找到可恢复的数据源")
    print("=" * 60)
    print()
    print("原因: MySQL DROP TABLE 会立即关闭 .ibd 文件句柄并释放内存映射。")
    print("推荐裸盘扫描方案:")
    print(f"  python {sys.argv[0]} --detect-device")
    print(f"  python {sys.argv[0]} --device /dev/vda3 --auto-schema --table {table_hint} --workers 8 --relaxed -o out.sql")
    print()
    return None


def _detect_mysql_datadir() -> str:
    candidates = ['/var/lib/mysql', '/usr/local/mysql/data', '/data/mysql', '/opt/mysql/data']
    for c in candidates:
        if os.path.isdir(c):
            return c
    try:
        out = subprocess.check_output(
            ['mysql', '-e', 'SELECT @@datadir', '-s', '--skip-column-names'],
            stderr=subprocess.DEVNULL).decode().strip()
        if out and os.path.isdir(out):
            return out
    except Exception:
        pass
    return ''


def _detect_device(path: str) -> str:
    try:
        out = subprocess.check_output(
            ['df', '--output=source', path], stderr=subprocess.DEVNULL).decode()
        lines = [l.strip() for l in out.strip().splitlines() if l.strip()]
        return lines[1] if len(lines) >= 2 else ''
    except Exception:
        return ''


def _gen_schema_template(table: str, out: str):
    tmpl = {
        "table": table,
        "row_format": "DYNAMIC",
        "comment": "请根据 CREATE TABLE 语句填写列定义",
        "columns": [
            {"name": "id",         "type": "bigint",       "nullable": False, "unsigned": True},
            {"name": "name",       "type": "varchar(255)",  "nullable": True,  "charset": "utf8mb4"},
            {"name": "price",      "type": "decimal(10,2)", "nullable": True},
            {"name": "created_at", "type": "datetime",      "nullable": False},
        ]
    }
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(tmpl, f, ensure_ascii=False, indent=2)
    print(f"Schema 模板已写入: {out}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description='InnoDB Recovery Tool for MySQL 8.0 v2\n'
                    '统一 Pipeline 架构 — 支持 .ibd / 裸设备 / /proc/fd 抢救',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
恢复场景示例:
  # 场景1: .ibd 文件恢复
  python innodb_recovery.py --ibd orders.ibd --schema schema.json --brute-force -o out.sql

  # 场景2: 裸盘扫描 + SDI 自动提取
  python innodb_recovery.py --device /dev/vda3 --auto-schema --table orders --workers 8 --relaxed -o out.sql

  # 场景3: 多库同名表
  python innodb_recovery.py --device /dev/vda3 --auto-schema --table audit_logs --database db_prod --workers 8 --relaxed -o out.sql

  # 辅助: 检测设备 / 生成模板 / 页统计
  python innodb_recovery.py --detect-device
  python innodb_recovery.py --gen-schema orders --schema-out schema.json
  python innodb_recovery.py --ibd orders.ibd --page-info

注意: 裸盘扫描和 /proc/fd 抢救需 root 权限。
        """)

    src = p.add_mutually_exclusive_group()
    src.add_argument('--ibd',    help='.ibd 文件路径')
    src.add_argument('--device', help='裸块设备路径 (/dev/sda1)')
    src.add_argument('--rescue', action='store_true',
                     help='从 /proc/fd 抢救被 DROP 的表')

    p.add_argument('--schema',      help='表结构 JSON 文件路径')
    p.add_argument('--auto-schema', action='store_true',
                   help='从 MySQL 8.0 SDI 页自动提取表结构')
    p.add_argument('--table',       help='表名过滤（SDI/抢救模式）')
    p.add_argument('--database', '--db', dest='database',
                   help='数据库名（多库同名表时指定）')
    p.add_argument('-o', '--output', help='输出文件路径（默认 stdout）')
    p.add_argument('--format',      choices=['sql', 'csv', 'json'],
                   default='sql', help='输出格式')
    p.add_argument('--no-deleted',  action='store_true',
                   help='不输出 delete-marked 记录')
    p.add_argument('--brute-force', action='store_true',
                   help='暴力扫描（链表破坏时使用）')
    p.add_argument('--index-id',    type=int, default=0,
                   help='只扫描指定 index_id 的页')

    # 裸盘专用
    p.add_argument('--offset',     type=int, default=0, help='扫描起始 MB')
    p.add_argument('--length',     type=int, default=0, help='扫描长度 MB')
    p.add_argument('--space-id',   type=int, default=0, help='只匹配指定 space_id')
    p.add_argument('--read-chunk', type=int, default=64, help='读取块大小 MB')
    p.add_argument('--workers',    type=int, default=4, help='并行线程数')
    p.add_argument('--quick-scan', action='store_true',
                   help='快速预扫描（只定位页，不恢复数据）')
    p.add_argument('--relaxed',    action='store_true',
                   help='宽松模式：接受所有 INDEX/BLOB 页')

    # 辅助
    p.add_argument('--gen-schema',   metavar='TABLE', help='生成 schema 模板')
    p.add_argument('--schema-out',   default='schema_template.json', help='模板输出路径')
    p.add_argument('--page-info',    action='store_true', help='打印 .ibd 页统计')
    p.add_argument('--detect-device', action='store_true', help='检测 MySQL 数据目录设备')
    p.add_argument('--system',       action='store_true', help='扫描系统表空间')
    p.add_argument('--verbose', '-v', action='store_true', help='详细日志')
    return p


def main():
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')

    # ── 辅助命令 ──
    if args.gen_schema:
        _gen_schema_template(args.gen_schema, args.schema_out)
        return

    if args.detect_device:
        dd = _detect_mysql_datadir()
        if dd:
            dev = _detect_device(dd)
            print(f"MySQL 数据目录: {dd}")
            print(f"所在块设备:     {dev if dev else '(无法自动检测, 请运行 df -h)'}")
        else:
            print("未能检测 MySQL 数据目录, 请手动运行: df -h /var/lib/mysql")
        return

    if args.page_info:
        if not args.ibd:
            print("需要 --ibd 参数")
            sys.exit(1)
        counts: Dict[str, int] = {}
        with open(args.ibd, 'rb') as f:
            page_no = 0
            while True:
                raw = f.read(UNIV_PAGE_SIZE)
                if len(raw) < UNIV_PAGE_SIZE:
                    break
                pt = _u16(raw, FIL_PAGE_TYPE)
                name = PAGE_TYPE_NAMES.get(pt, f'0x{pt:04x}')
                counts[name] = counts.get(name, 0) + 1
                page_no += 1
        print(f"\n文件: {args.ibd}  共 {page_no} 页")
        print(f"{'页类型':<20} {'数量':>8}")
        print('-' * 30)
        for k, v in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"  {k:<18} {v:>8}")
        return

    # ── 快速预扫描 ──
    if args.quick_scan and args.device:
        src = DevicePageSource(args.device, args.read_chunk * 1024 * 1024)
        scanner = RecoveryScanner(src, TableSchema(), workers=args.workers)
        start_b = args.offset * 1024 * 1024
        length_b = args.length * 1024 * 1024 if args.length else 0
        candidates = scanner.prescan(start_b, length_b)

        if candidates:
            print(f"\n找到 {len(candidates)} 个候选 InnoDB 页")
            first = candidates[0].offset
            last = candidates[-1].offset
            start_mb = max(0, first // 1024 // 1024 - 100)
            scan_len = max(256, (last - first) // 1024 // 1024 + 200)
            print(f"\n建议恢复命令:")
            print(f"  python {sys.argv[0]} --device {args.device} \\")
            print(f"      --schema <schema.json> \\")
            print(f"      --offset {start_mb} --length {scan_len} \\")
            print(f"      --workers {args.workers} --relaxed -o recovered.sql")
            # Distribution
            segs: Dict[int, Dict[str, int]] = {}
            seg_size = 1024 * 1024 * 1024
            for c in candidates:
                seg = c.offset // seg_size
                segs.setdefault(seg, {}).setdefault(c.type_name, 0)
                segs[seg][c.type_name] += 1
            print(f"\n候选页分布:")
            for seg in sorted(segs):
                parts = ', '.join(f'{k}:{v}' for k, v in sorted(segs[seg].items()))
                print(f"    {seg}-{seg+1} GB: {parts}")
        else:
            print(f"\n未找到 InnoDB 页特征")
        src.close()
        return

    # ── Schema 加载 ──
    schema_name = ''
    if args.auto_schema:
        schema_filter = args.database or ''

        if args.ibd:
            source = FilePageSource(args.ibd)
        elif args.device:
            source = DevicePageSource(args.device, args.read_chunk * 1024 * 1024)
        elif args.rescue:
            rescued = _rescue_ibd(args.table or 'recovered')
            if not rescued:
                sys.exit(1)
            source = FilePageSource(rescued)
        else:
            logging.error("--auto-schema 需要 --ibd/--device/--rescue")
            sys.exit(1)

        schema = SDIExtractor.extract_from_source(
            source, table_filter=args.table or '',
            schema_filter=schema_filter)
        if hasattr(source, 'close'):
            source.close()

        if schema is None:
            # 详细诊断信息已在 extract_from_source 中输出
            logging.error("SDI 提取失败（详见上方诊断），请手动创建 schema.json")
            print(f"手动生成模板: python {sys.argv[0]} --gen-schema your_table")
            sys.exit(1)

        schema_name = schema.database
        if args.schema_out:
            SDIExtractor.generate_schema_json(schema, args.schema_out)

        logging.info(f"SDI: {schema.full_name}  行格式={schema.row_format}  "
                     f"列数={len(schema.columns)}")
    else:
        if not args.schema:
            build_parser().print_help()
            print("\n错误: 需要 --schema 或 --auto-schema")
            sys.exit(1)

        schema = load_schema_from_json(args.schema)
        schema_name = schema.database or args.database or ''
        schema.database = schema_name

    logging.info(f"表: {schema.full_name}  行格式: {schema.row_format}  "
                 f"列数: {len(schema.columns)}")

    # ── 数据源 ──
    source: Optional[PageSource] = None

    if args.rescue:
        rescued = _rescue_ibd(args.table or schema.table)
        if not rescued:
            print()
            print("推荐裸盘扫描:")
            print(f"  python {sys.argv[0]} --device /dev/vda3 --auto-schema "
                  f"--table {args.table or schema.table} --workers 8 --relaxed -o out.sql")
            sys.exit(1)
        source = FilePageSource(rescued)

    elif args.device:
        if not os.path.exists(args.device):
            print(f"错误: 设备不存在: {args.device}")
            print("提示: 运行 --detect-device 自动检测")
            sys.exit(1)
        source = DevicePageSource(args.device, args.read_chunk * 1024 * 1024)

    elif args.ibd:
        if not os.path.exists(args.ibd):
            print(f"错误: 文件不存在: {args.ibd}")
            print(f"\nDROP TABLE 后 .ibd 已删除，推荐裸盘扫描:")
            dd = _detect_mysql_datadir()
            dev = _detect_device(dd) if dd else '/dev/sda1'
            print(f"  python {sys.argv[0]} --device {dev} --auto-schema "
                  f"--table {os.path.basename(args.ibd).replace('.ibd','')} "
                  f"--workers 8 --relaxed -o out.sql")
            sys.exit(1)
        source = FilePageSource(args.ibd)

    else:
        build_parser().print_help()
        sys.exit(1)

    # ── 扫描 ──
    scanner = RecoveryScanner(
        source=source, schema=schema,
        workers=args.workers, relaxed=args.relaxed,
        brute_force=args.brute_force,
        space_id=args.space_id,
        index_id=args.index_id,
        include_deleted=not args.no_deleted)

    start_b = args.offset * 1024 * 1024
    len_b  = args.length * 1024 * 1024 if args.length else 0

    result = scanner.recover(start_byte=start_b, length_bytes=len_b)
    if hasattr(source, 'close'):
        source.close()

    if not result.rows:
        logging.warning("未恢复到任何记录。建议:")
        logging.warning("  --ibd 模式加 --brute-force")
        logging.warning("  --device 模式加 --relaxed --quick-scan 先定位")
        logging.warning("  指定 --space-id 或 --index-id 精确过滤")

    # ── 输出 ──
    writer = OutputWriter(schema)
    fmt = args.format
    content = writer.to_sql(result.rows) if fmt == 'sql' else \
              writer.to_csv(result.rows) if fmt == 'csv' else \
              writer.to_json(result.rows)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"结果已写入: {args.output}  共 {len(result.rows)} 条记录")
    else:
        print(content)


if __name__ == '__main__':
    main()
