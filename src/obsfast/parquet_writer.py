"""对象元数据导出——Parquet 写入。

导出格式与消费方约定保持一致：

===========  =========  ==============================================
字段          类型        说明
===========  =========  ==============================================
``Key``      UTF-8      完整对象键
``Size``     UInt64     对象大小（字节）
``LastModified``  UInt64 最后修改时间（Unix 秒）
``ETag``     UTF-8      内容标识，分段上传时形如 ``<md5>-<段数>``
``DiffFlag`` UInt8      0 两侧一致 / 1 仅源桶 / 2 仅目标桶 / 3 两侧不一致
===========  =========  ==============================================

写出参数：Parquet 1.0、GZIP（级别 6）、PLAIN 编码（关闭字典编码）。这些参数
保证了不同 Parquet 实现对文件的兼容读取。

若运行环境未安装 ``pyarrow``，导出会退化为「跳过 Parquet 并给出明确提示」，
前缀分布文件仍会正常产出——工具的核心能力（快速盘点）不因此丢失。
"""

from __future__ import annotations

import logging

from .model import MatchResult, ObjectProps

logger = logging.getLogger("obsfast.parquet")

#: 各 DiffFlag 取值的中文说明，用于日志与文档
DIFF_FLAG_LABELS = {
    int(MatchResult.EQUAL): "两侧一致",
    int(MatchResult.PLUS): "仅源桶存在",
    int(MatchResult.MINUS): "仅目标桶存在",
    int(MatchResult.ASTRISK): "两侧都有但元数据不一致",
}

GZIP_LEVEL = 6


def pyarrow_available() -> bool:
    """探测 PyArrow 是否可用。"""
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


class ParquetSink:
    """按 ``DiffFlag`` 分批追加写入的 Parquet 输出。

    用法::

        with ParquetSink("out.parquet") as sink:
            sink.write(rows, diff_flag=1)
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._writer = None
        self._schema = None
        self._rows_written = 0
        self._enabled = pyarrow_available()
        if not self._enabled:
            logger.warning(
                "未检测到 PyArrow，跳过对象元数据导出（%s）；前缀分布文件不受影响。",
                path,
            )

    # -- 生命周期 -----------------------------------------------------------

    def __enter__(self) -> "ParquetSink":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def open(self) -> None:
        if not self._enabled:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        self._schema = pa.schema(
            [
                pa.field("Key", pa.string(), nullable=False),
                pa.field("Size", pa.uint64(), nullable=False),
                pa.field("LastModified", pa.uint64(), nullable=False),
                pa.field("ETag", pa.string(), nullable=False),
                pa.field("DiffFlag", pa.uint8(), nullable=False),
            ]
        )
        self._writer = pq.ParquetWriter(
            self.path,
            self._schema,
            version="1.0",
            compression="GZIP",
            compression_level=GZIP_LEVEL,
            use_dictionary=False,
            write_statistics=True,
        )

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            logger.info("对象元数据已导出至 %s（%d 行）", self.path, self._rows_written)

    # -- 写入 ---------------------------------------------------------------

    def write(self, rows: list[tuple[str, ObjectProps]], diff_flag: int) -> int:
        """写入一批同 ``DiffFlag`` 的对象元数据，返回写入行数。"""
        if not rows:
            return 0
        if not self._enabled:
            return 0
        if self._writer is None:
            self.open()

        import pyarrow as pa

        keys = [key for key, _ in rows]
        sizes = [props.size for _, props in rows]
        modified = [props.last_modified for _, props in rows]
        etags = [props.etag_string() for _, props in rows]
        flags = [diff_flag] * len(rows)

        batch = pa.record_batch(
            [
                pa.array(keys, type=pa.string()),
                pa.array(sizes, type=pa.uint64()),
                pa.array(modified, type=pa.uint64()),
                pa.array(etags, type=pa.string()),
                pa.array(flags, type=pa.uint8()),
            ],
            schema=self._schema,
        )
        self._writer.write_batch(batch)
        self._rows_written += len(rows)
        return len(rows)

    def write_groups(self, groups: dict[int, list[tuple[str, ObjectProps]]]) -> int:
        """一次性写入按 ``DiffFlag`` 分组的全部结果。"""
        total = 0
        for flag in sorted(groups):
            rows = groups[flag]
            written = self.write(rows, flag)
            if written:
                logger.info(
                    "  导出 %d 条「%s」记录（DiffFlag=%d）",
                    written,
                    DIFF_FLAG_LABELS.get(flag, "未知"),
                    flag,
                )
            total += written
        return total

    @property
    def rows_written(self) -> int:
        return self._rows_written

    @property
    def active(self) -> bool:
        return self._enabled


def export_metadata(path: str, groups: dict[int, list[tuple[str, ObjectProps]]]) -> int:
    """便捷入口：把分组结果一次性导出为 Parquet 文件。"""
    with ParquetSink(path) as sink:
        return sink.write_groups(groups)
