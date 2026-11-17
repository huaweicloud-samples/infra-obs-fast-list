"""桶清单——配置管理、清单文件解析与「清单 → 前缀分布」转换。

桶清单由对象存储服务在服务端按日/周周期生成，产物落在指定目标桶的::

    {目标前缀}/{源桶名}/{清单名称}/{yyyy-MM-ddTHH-mmZ}/manifest.json

目录下，同级的 ``files/`` 子目录存放数据文件。:class:`InventoryReport`
负责解析 ``manifest.json``；:func:`inventory_to_prefix_map` 负责并发下载数据
文件、逐行提取对象键、汇总为前缀分布并落盘。

清单解析部分有两处刻意偏离「按固定格式假设」的常规写法，均为提升健壮性所必需：

1. **键所在列按 ``fileSchema`` 动态定位**，而不是写死第 2 列。清单的可选字段
   会影响列序，固定下标在字段集合变化时会静默取到错误数据。
2. **数据文件是否压缩按内容识别**（gzip 魔数 ``1f 8b``），而不是按扩展名。
   服务端不同版本导出的文件名后缀并不统一。
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Iterable
from xml.sax.saxutils import escape

from .data_map import format_ks_line
from .obs_client import ObsEndpoint

logger = logging.getLogger("obsfast.inventory")

#: 支持的清单频率
ALLOWED_FREQUENCIES = ("Daily", "Weekly")
#: 支持的版本范围
ALLOWED_VERSIONS = ("Current", "All")
#: 支持的可选字段
ALLOWED_OPTIONAL_FIELDS = (
    "Size",
    "LastModifiedDate",
    "ETag",
    "StorageClass",
    "IsMultipartUploaded",
    "ReplicationStatus",
    "EncryptionStatus",
)

_XML_NS = "http://obs.myhwclouds.com/doc/2015-06-30/"


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass
class InventoryConfiguration:
    """一条桶清单配置。"""

    inventory_id: str
    destination_bucket: str
    enabled: bool = True
    frequency: str = "Daily"
    included_versions: str = "Current"
    destination_prefix: str = ""
    filter_prefix: str = ""
    optional_fields: tuple[str, ...] = (
        "Size",
        "LastModifiedDate",
        "ETag",
        "StorageClass",
        "IsMultipartUploaded",
    )
    file_format: str = "CSV"

    def validate(self) -> None:
        if not self.inventory_id:
            raise ValueError("清单名称不能为空")
        if len(self.inventory_id) > 64:
            raise ValueError("清单名称最长为 64 个字符")
        if not self.destination_bucket:
            raise ValueError("必须指定保存清单文件的目标桶")
        if self.frequency not in ALLOWED_FREQUENCIES:
            raise ValueError(
                f"生成频率仅支持 {'/'.join(ALLOWED_FREQUENCIES)}，收到 {self.frequency!r}"
            )
        if self.included_versions not in ALLOWED_VERSIONS:
            raise ValueError(
                f"版本范围仅支持 {'/'.join(ALLOWED_VERSIONS)}，收到 {self.included_versions!r}"
            )
        invalid = [f for f in self.optional_fields if f not in ALLOWED_OPTIONAL_FIELDS]
        if invalid:
            raise ValueError(f"不支持的可选字段: {', '.join(invalid)}")
        if self.file_format.upper() != "CSV":
            raise ValueError("清单文件仅支持 CSV 格式")

    def to_xml(self) -> bytes:
        """序列化为 ``PutBucketInventory`` 的请求体。"""
        self.validate()
        parts = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            f'<InventoryConfiguration xmlns="{_XML_NS}">',
            f"  <Id>{escape(self.inventory_id)}</Id>",
            f"  <IsEnabled>{'true' if self.enabled else 'false'}</IsEnabled>",
        ]
        if self.filter_prefix:
            parts.append("  <Filter>")
            parts.append(f"    <Prefix>{escape(self.filter_prefix)}</Prefix>")
            parts.append("  </Filter>")
        parts.append("  <Destination>")
        parts.append(f"    <Bucket>{escape(self.destination_bucket)}</Bucket>")
        if self.destination_prefix:
            parts.append(f"    <Prefix>{escape(self.destination_prefix)}</Prefix>")
        parts.append("    <Format>CSV</Format>")
        parts.append("  </Destination>")
        parts.append("  <Schedule>")
        parts.append(f"    <Frequency>{self.frequency}</Frequency>")
        parts.append("  </Schedule>")
        parts.append(
            f"  <IncludedObjectVersions>{self.included_versions}</IncludedObjectVersions>"
        )
        if self.optional_fields:
            parts.append("  <OptionalFields>")
            for item in self.optional_fields:
                parts.append(f"    <Field>{item}</Field>")
            parts.append("  </OptionalFields>")
        parts.append("</InventoryConfiguration>")
        return "\n".join(parts).encode("utf-8")


def _parse_inventory_xml(payload: bytes) -> InventoryConfiguration:
    import xml.etree.ElementTree as ET

    from .obs_client import _find, _text  # 复用命名空间无关的查找逻辑

    root = ET.fromstring(payload)
    destination = _find(root, "Destination")
    schedule = _find(root, "Schedule")
    optional = _find(root, "OptionalFields")
    filter_node = _find(root, "Filter")

    fields = tuple(
        (item.text or "").strip()
        for item in (optional if optional is not None else [])
        if item.text
    )

    return InventoryConfiguration(
        inventory_id=_text(root, "Id"),
        destination_bucket=_text(destination, "Bucket") if destination is not None else "",
        enabled=_text(root, "IsEnabled", "true").lower() == "true",
        frequency=_text(schedule, "Frequency", "Daily") if schedule is not None else "Daily",
        included_versions=_text(root, "IncludedObjectVersions", "Current"),
        destination_prefix=_text(destination, "Prefix") if destination is not None else "",
        filter_prefix=_text(filter_node, "Prefix") if filter_node is not None else "",
        optional_fields=fields,
        file_format=_text(destination, "Format", "CSV") if destination is not None else "CSV",
    )


# ---------------------------------------------------------------------------
# 清单配置的增删查
# ---------------------------------------------------------------------------


def put_inventory(
    endpoint: ObsEndpoint, bucket: str, config: InventoryConfiguration
) -> None:
    """创建或覆盖一条桶清单配置。"""
    body = config.to_xml()
    response = endpoint.request(
        "PUT",
        bucket,
        query={"inventory": [""], "id": [config.inventory_id]},
        body=body,
        content_type="application/xml",
    )
    endpoint.raise_for_status(response, f"配置桶清单 {config.inventory_id}")


def get_inventory(
    endpoint: ObsEndpoint, bucket: str, inventory_id: str
) -> InventoryConfiguration:
    """读取指定桶清单配置。"""
    response = endpoint.request(
        "GET", bucket, query={"inventory": [""], "id": [inventory_id]}
    )
    endpoint.raise_for_status(response, f"读取桶清单 {inventory_id}")
    return _parse_inventory_xml(response.body)


def list_inventories(endpoint: ObsEndpoint, bucket: str) -> list[InventoryConfiguration]:
    """列出桶上已配置的全部清单。"""
    import xml.etree.ElementTree as ET

    from .obs_client import _iter_children

    response = endpoint.request("GET", bucket, query={"inventory": [""]})
    endpoint.raise_for_status(response, f"列举桶清单 {bucket}")
    if not response.body:
        return []
    root = ET.fromstring(response.body)
    configs: list[InventoryConfiguration] = []
    for node in _iter_children(root, "InventoryConfiguration"):
        configs.append(_parse_inventory_xml(ET.tostring(node)))
    return configs


def delete_inventory(endpoint: ObsEndpoint, bucket: str, inventory_id: str) -> None:
    """删除指定桶清单配置。"""
    response = endpoint.request(
        "DELETE", bucket, query={"inventory": [""], "id": [inventory_id]}
    )
    if response.status not in (204, 404):
        endpoint.raise_for_status(response, f"删除桶清单 {inventory_id}")


# ---------------------------------------------------------------------------
# 清单报告
# ---------------------------------------------------------------------------


@dataclass
class InventoryFile:
    """清单报告中的一个数据文件。"""

    key: str
    size: int = 0
    records: int = 0


@dataclass
class InventoryReport:
    """``manifest.json`` 的解析结果。"""

    source_bucket: str
    destination_bucket: str
    version: str = ""
    file_format: str = "CSV"
    file_schema: str = ""
    files: list[InventoryFile] = field(default_factory=list)

    @classmethod
    def parse(cls, payload: bytes) -> "InventoryReport":
        data = json.loads(payload.decode("utf-8"))
        files = [
            InventoryFile(
                key=urllib.parse.unquote(item.get("key", "")),
                size=int(item.get("size") or 0),
                records=int(item.get("inventoriedRecord") or item.get("inventoried_record") or 0),
            )
            for item in data.get("files", [])
        ]
        return cls(
            source_bucket=data.get("sourceBucket", ""),
            destination_bucket=data.get("destinationBucket", ""),
            version=str(data.get("version", "")),
            file_format=str(data.get("fileFormat", "CSV")),
            file_schema=str(data.get("fileSchema", "")),
            files=[item for item in files if item.key],
        )

    @property
    def key_column_index(self) -> int:
        """定位 ``Key`` 所在列；缺失时回退到第 2 列。"""
        if not self.file_schema:
            return 1
        columns = [column.strip() for column in self.file_schema.split(",")]
        try:
            return columns.index("Key")
        except ValueError:
            return 1

    @property
    def total_records(self) -> int:
        return sum(item.records for item in self.files)


def parse_manifest_location(location: str) -> tuple[str, str]:
    """把清单文件位置解析为 ``(桶, 对象键)``。

    接受三种写法：

    * ``obs://bucket/path/to/manifest.json``
    * ``bucket/path/to/manifest.json``
    * ``/path/to/manifest.json``（桶由 ``--bucket`` 指定，此处仅用于键）
    """
    value = location.strip()
    if value.startswith("obs://"):
        rest = value[len("obs://") :]
        bucket, _, key = rest.partition("/")
        if not bucket:
            raise ValueError(f"无法从 {location!r} 解析出桶名")
        return bucket, key
    if value.startswith("/"):
        raise ValueError(
            f"{location!r} 未包含桶名，请使用 obs://<桶>/<对象键> 形式或配合 --bucket 使用"
        )
    bucket, _, key = value.partition("/")
    if not bucket or not key:
        raise ValueError(f"无法从 {location!r} 解析出桶与对象键")
    return bucket, key


def _maybe_gunzip(payload: bytes) -> bytes:
    """按内容识别 gzip 并解压。"""
    if len(payload) >= 2 and payload[0] == 0x1F and payload[1] == 0x8B:
        return gzip.decompress(payload)
    return payload


def _extract_keys(payload: bytes, key_index: int) -> list[str]:
    """从清单数据文件中提取对象键列（按行处理，容忍脏行）。"""
    text = _maybe_gunzip(payload).decode("utf-8", "replace")
    reader = csv.reader(io.StringIO(text))
    keys: list[str] = []
    for row in reader:
        if not row or len(row) <= key_index:
            continue
        raw = row[key_index]
        if not raw:
            continue
        # 清单中的对象键是 URL 编码的
        keys.append(urllib.parse.unquote(raw))
    return keys


def inventory_prefix_counts(
    endpoint: ObsEndpoint,
    manifest_location: str,
    concurrency: int = 1,
    progress=None,
) -> tuple[dict[str, int], InventoryReport]:
    """从清单报告汇总 ``{前缀: 对象数}`` 并返回清单报告本身。

    :param concurrency: 同时下载的数据文件数量
    :param progress: 可选的 ``(已完成, 总数)`` 回调，用于展示进度
    """
    bucket, key = parse_manifest_location(manifest_location)
    report = InventoryReport.parse(endpoint.get_object(bucket, key))
    if report.file_format.upper() != "CSV":
        raise ValueError(f"仅支持 CSV 格式清单，当前为 {report.file_format!r}")
    logger.info(
        "清单报告：源桶 %s，目标桶 %s，数据文件 %d 个，记录 %d 条",
        report.source_bucket,
        report.destination_bucket,
        len(report.files),
        report.total_records,
    )

    key_index = report.key_column_index
    total = len(report.files)
    done = 0

    def handle(item: InventoryFile) -> dict[str, int]:
        body = endpoint.get_object(bucket, item.key)
        local: dict[str, int] = {}
        for object_key in _extract_keys(body, key_index):
            prefix = object_key.rpartition("/")[0] or "/"
            local[prefix] = local.get(prefix, 0) + 1
        return local

    partials: list[dict[str, int]] = []
    if concurrency <= 1 or total <= 1:
        for item in report.files:
            partials.append(handle(item))
            done += 1
            if progress is not None:
                progress(done, total)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for result in pool.map(handle, report.files):
                partials.append(result)
                done += 1
                if progress is not None:
                    progress(done, total)

    counts: dict[str, int] = {}
    for partial in partials:
        for prefix, value in partial.items():
            counts[prefix] = counts.get(prefix, 0) + value
    return dict(sorted(counts.items())), report


def dump_prefix_counts(path: str, counts: dict[str, int]) -> tuple[int, int]:
    """把 ``{前缀: 对象数}`` 写为前缀分布文件，返回 ``(前缀数, 对象数)``。"""
    total = 0
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        for prefix in sorted(counts):
            count = counts[prefix]
            handle.write(format_ks_line(prefix, count) + "\n")
            total += count
    return len(counts), total


def loads_keys(payload: bytes, key_index: int = 1) -> Iterable[str]:
    """便捷函数：从清单数据文件字节流中提取对象键。"""
    return _extract_keys(payload, key_index)
