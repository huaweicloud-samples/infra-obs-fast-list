"""数据聚合层——键空间切分、前缀映射与对象比对落库。

三个核心角色：

* :class:`KeySpaceHints` —— 把「键空间提示」文件展开为若干互不重叠的
  ``[start, end)`` 区间，供并发盘点任务领取；字典序保证所有区间并集覆盖桶内
  全部对象且不重不漏；
* :class:`PrefixMap` —— 以「前缀 → 对象名 → 元数据」两级字典承接各任务回传的
  结果，负责导出前缀分布文件（``.ks``）与对象元数据文件（``.parquet``）；
* :class:`ObjectMap` —— 单个前缀下的对象集合，负责同名对象的合并与比对。
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator, Optional

from .filter_expr import ObjectFilter
from .model import (
    MatchResult,
    ObjectProps,
)

# ---------------------------------------------------------------------------
# 前缀分布文件的解析与写出
# ---------------------------------------------------------------------------


def parse_ks_line(line: str) -> Optional[tuple[str, int]]:
    """解析一行前缀分布记录，返回 ``(prefix, count)``。

    兼容两种历史写法：

    * 单桶盘点导出：``"prefix","count"``
    * 键空间切分导出：``"role","prefix","count"``

    空行与无法解析的行会被忽略，而不是中断整次解析——前缀分布文件常由人工
    编辑或从外部工具导出，容错比严格更有价值。
    """
    stripped = line.strip()
    if not stripped:
        return None
    fields = [part.strip().strip('"') for part in stripped.split(",")]
    if len(fields) == 2:
        prefix, count = fields
    elif len(fields) >= 3:
        prefix, count = fields[-2], fields[-1]
    else:
        return None
    try:
        return prefix, int(count)
    except ValueError:
        return None


def format_ks_line(prefix: str, count: int) -> str:
    """写出前缀分布记录（统一为 ``"prefix","count"`` 形式）。"""
    return f'"{prefix}","{count}"'


def load_hint_prefixes(path: str) -> list[str]:
    """读取「键空间提示」文件，返回去重、按字典序排序后的前缀列表。

    提示文件的格式是**每行一个前缀**（由 :command:`split` 子命令产出），因此不能
    直接套用前缀分布文件的 CSV 解析——那样每一行都会因字段数不足而被丢弃，
    导致提示文件形同虚设。为兼容历史上直接传入前缀分布文件的用法，本函数在
    遇到含逗号的行时回退到 :func:`parse_ks_line` 取首个字段。
    """
    prefixes: list[str] = []
    with open(path, "r", encoding="utf-8-sig") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            if "," in text:
                parsed = parse_ks_line(text)
                if parsed is not None:
                    prefixes.append(parsed[0])
                continue
            prefixes.append(text)
    return sorted(set(prefixes))


# ---------------------------------------------------------------------------
# 键空间提示
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KeySpacePair:
    """一个待盘点的键区间 ``[start, end)``；``end`` 为 ``None`` 表示「直到末尾」。"""

    index: int
    start: str
    end: Optional[str]

    @classmethod
    def build(cls, index: int, start: str, end: str) -> "KeySpacePair":
        return cls(index=index, start=start, end=end or None)

    def to_task_input(self) -> tuple[str, Optional[str]]:
        return self.start, self.end


class KeySpaceHints:
    """键空间提示队列。

    给定提示列表 ``[k1, k2, ...]``（已排序去重）会展开为 ``n + 1`` 个区间::

        ["", k1)
        [k1, k2)
        ...
        [kn, None)   # 到键空间末尾

    :meth:`next` 领取区间、:meth:`finish` 归还，用于控制并发度并统计在途任务。
    """

    def __init__(self, hints: Iterable[str]) -> None:
        items = sorted({hint for hint in hints if hint is not None})
        queue: deque[KeySpacePair] = deque()
        index = 0
        start = ""
        for key in items:
            queue.append(KeySpacePair.build(index, start, key))
            start = key
            index += 1
        queue.append(KeySpacePair.build(index, start, ""))
        self._queue = queue
        self._inflight: dict[int, KeySpacePair] = {}
        self._done: list[KeySpacePair] = []

    def next(self) -> Optional[KeySpacePair]:
        if not self._queue:
            return None
        pair = self._queue.popleft()
        self._inflight[pair.index] = pair
        return pair

    def finish(self, index: int) -> None:
        pair = self._inflight.pop(index, None)
        if pair is None:
            raise KeyError(f"区间 {index} 不在在途列表中")
        self._done.append(pair)

    def __len__(self) -> int:
        return len(self._queue)

    @property
    def pending(self) -> int:
        return len(self._queue)

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    @property
    def completed(self) -> int:
        return len(self._done)

    @property
    def total(self) -> int:
        return len(self._queue) + len(self._inflight) + len(self._done)


def split_prefixes(prefix_counts: dict[str, int], splits: int) -> list[str]:
    """按对象数量把前缀分布均匀切成 ``splits`` 段，返回切分点前缀列表。

    与「前缀分布文件 → 键空间提示」的转换保持一致：累计对象数每超过平均目标
    就在当前前缀处落一个切分点。当总量不足以切分时返回空列表，调用方据此跳过
    生成提示文件。
    """
    if splits <= 0:
        raise ValueError("切分段数必须为正整数")
    total = sum(prefix_counts.values())
    average = total // splits
    if average == 0:
        return []

    splits_out: list[str] = []
    cumulative = 0
    last_prefix = ""
    for prefix in sorted(prefix_counts):
        cumulative += prefix_counts[prefix]
        if cumulative > average:
            splits_out.append(last_prefix)
            cumulative = 0
        last_prefix = prefix
    return splits_out


# ---------------------------------------------------------------------------
# 对象集合与前缀映射
# ---------------------------------------------------------------------------


class ObjectMap:
    """单个前缀下的对象集合（对象名 → 元数据）。"""

    def __init__(self) -> None:
        self._items: dict[str, ObjectProps] = {}

    def __len__(self) -> int:
        return len(self._items)

    def bulk_insert(
        self,
        items: Iterable[tuple[str, ObjectProps]],
        filter_: Optional[ObjectFilter] = None,
        compare_etag: bool = True,
    ) -> list[str]:
        """批量写入并完成同名对象比对，返回需要标记为「差异」的键列表。"""
        astrisk: list[str] = []
        for name, props in items:
            existing = self._items.get(name)
            if existing is None:
                self._items[name] = props
                continue
            result = existing.match(props, filter_, compare_etag)
            if result is MatchResult.ASTRISK:
                astrisk.append(name)
        return astrisk

    def items(self) -> Iterator[tuple[str, ObjectProps]]:
        return iter(self._items.items())


class PrefixMap:
    """前缀 → 对象集合的两级映射。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inner: dict[str, ObjectMap] = {}

    def get_object_map(self, prefix: str) -> ObjectMap:
        with self._lock:
            obj_map = self._inner.get(prefix)
            if obj_map is None:
                obj_map = ObjectMap()
                self._inner[prefix] = obj_map
            return obj_map

    def ingest(self, batches: Iterable[dict[str, list[tuple[str, ObjectProps]]]],
               filter_: Optional[ObjectFilter] = None,
               compare_etag: bool = True) -> int:
        """吸收若干批次数据，返回写入的对象条目数。"""
        written = 0
        for batch in batches:
            for prefix, items in batch.items():
                obj_map = self.get_object_map(prefix)
                obj_map.bulk_insert(items, filter_, compare_etag)
                written += len(items)
        return written

    def apply(self, batch: dict[str, list[tuple[str, ObjectProps]]],
              filter_: Optional[ObjectFilter] = None,
              compare_etag: bool = True) -> None:
        """吸收单批数据（作用与 :meth:`ingest` 相同，便于调用方按需选择）。"""
        for prefix, items in batch.items():
            self.get_object_map(prefix).bulk_insert(items, filter_, compare_etag)

    # -- 统计 ---------------------------------------------------------------

    @property
    def prefix_count(self) -> int:
        with self._lock:
            return len(self._inner)

    @property
    def object_count(self) -> int:
        with self._lock:
            return sum(len(obj_map) for obj_map in self._inner.values())

    def __str__(self) -> str:
        return f"prefix count {self.prefix_count}, object count {self.object_count}"

    # -- 导出 ---------------------------------------------------------------

    def prefix_counts(self) -> dict[str, int]:
        """返回排序后的 ``{prefix: count}``，用于写出前缀分布文件。"""
        with self._lock:
            snapshot = {prefix: len(m) for prefix, m in self._inner.items()}
        return dict(sorted(snapshot.items()))

    def dump_ks(self, path: str) -> tuple[int, int]:
        """写出前缀分布文件，返回 ``(前缀数, 对象数)``。"""
        counts = self.prefix_counts()
        total = 0
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            for prefix, count in counts.items():
                handle.write(format_ks_line(prefix, count) + "\n")
                total += count
        return len(counts), total

    def diff_groups(
        self,
        include_equal: bool = False,
        filter_: Optional[ObjectFilter] = None,
        encode_key: Optional[Callable[[str, str], str]] = None,
    ) -> dict[int, list[tuple[str, ObjectProps]]]:
        """把所有对象按 ``DiffFlag`` 分组。

        :param include_equal: 是否包含「两侧一致」的对象（比对模式默认不导出）
        :param filter_: 单桶模式下的过滤条件
        :param encode_key: 由 ``(prefix, name)`` 还原完整对象键的函数
        """
        encode = encode_key or (lambda prefix, name: encode_key_default(prefix, name))
        groups: dict[int, list[tuple[str, ObjectProps]]] = {
            int(MatchResult.PLUS): [],
            int(MatchResult.MINUS): [],
            int(MatchResult.ASTRISK): [],
            int(MatchResult.EQUAL): [],
        }
        with self._lock:
            snapshot = list(self._inner.items())
        for prefix, obj_map in snapshot:
            for name, props in obj_map.items():
                result = props.final_status_check(filter_)
                if result is MatchResult.IGNORE:
                    continue
                if result is MatchResult.EQUAL and not include_equal:
                    continue
                groups[int(result)].append((encode(prefix, name), props))
        return groups


def encode_key_default(prefix: str, name: str) -> str:
    """由 ``(前缀, 对象名)`` 还原完整对象键；根前缀写作 ``"/"``。"""
    if prefix in ("", "/"):
        return name
    return f"{prefix}/{name}" if name else prefix
