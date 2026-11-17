"""对象元数据模型、运行模式、匹配语义与错误分类。

本模块集中定义「盘点」过程中流转的全部数据结构：

* :class:`ObjectProps` —— 单个对象的元数据（ETag / 大小 / 修改时间）及其比对状态；
* :class:`RunMode` —— 单桶盘点（``list``）或双桶比对（``diff``）；
* :class:`MatchResult` —— 双桶比对时单个键的匹配结论；
* :class:`FastListError` —— 运行期错误及其「可否继续」判定。

对外导出的 parquet 字段与 :class:`ObjectProps` 的映射见 :meth:`ObjectProps.to_row`。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Optional

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024

#: 运行期心跳输出间隔（秒）
DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS = 5
#: 任务全部完成后额外等待窗口（秒），用于冲掉尚未落库的批次
DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS = 1
#: 单次列举请求的响应超时（秒）
DEFAULT_OBS_LIST_TIMEOUT = 30
#: 列举接口单页上限（对象存储服务固定为 1000）
OBS_MAX_KEYS_PER_PAGE = 1000

#: 侧别标志位——左侧（源桶）
FLAG_DIR_LEFT = 0b1000_0000
#: 侧别标志位——右侧（目标桶）
FLAG_DIR_RIGHT = 0b0100_0000
#: 侧别标志位掩码——两侧都出现
FLAG_DIR_BOTH = FLAG_DIR_LEFT | FLAG_DIR_RIGHT
#: 比对模式标志位
FLAG_DIFF_MODE = 0b0010_0000

#: 对象状态——待判定
STATUS_OPEN = 0xFF
#: 对象状态——两侧一致
STATUS_MATCH = 0x00
#: 对象状态——大小不一致
STATUS_SIZE_NOT_MATCH = 1
#: 对象状态——ETag 缺失
STATUS_ETAG_NOT_AVAIL = 2
#: 对象状态——ETag 不一致
STATUS_ETAG_NOT_MATCH = 3
#: 对象状态——被过滤条件排除
STATUS_FILTER_OUT = 4


class RunMode(str, Enum):
    """运行模式。"""

    LIST = "list"
    BIDIR = "diff"


class MatchResult(IntEnum):
    """双桶比对结论。

    数值同时作为 parquet ``DiffFlag`` 列的取值。
    """

    EQUAL = 0
    PLUS = 1
    MINUS = 2
    ASTRISK = 3
    DUP = 4
    IGNORE = 5


class TaskKind(IntEnum):
    """参与启动同步的任务类别（用于会合屏障）。"""

    LEFT_LIST = 0
    RIGHT_LIST = 1
    AGGREGATOR = 2
    MONITOR = 3


# ---------------------------------------------------------------------------
# 错误分类
# ---------------------------------------------------------------------------

#: 下一页列举超时——可原地续跑
ERR_NEXT_STREAM_TIMEOUT = 0x01
#: 客户端通用异常——可原地续跑
ERR_CLIENT_GENERIC = 0x02
#: 连接超时——可原地续跑
ERR_CLIENT_CONNECTION_TIMEOUT = 0x03
#: 区域缺失或端点不可达——可原地续跑
ERR_MISSING_REGION = 0x04
#: 桶不存在——终止
ERR_NO_BUCKET = 0x10
#: 鉴权失败——终止
ERR_ACCESS_DENIED = 0x11
#: 命中重定向（桶与端点区域不匹配）——终止
ERR_PERMANENT_REDIRECT = 0x12
#: 未归类错误——终止
ERR_UNKNOWN = 0xFF

#: 小于该值的错误码视为「可续跑」
_CONTINUABLE_BELOW = ERR_NO_BUCKET


class FastListError(Exception):
    """盘点过程中的可分类错误。

    :param errno: 上述 ``ERR_*`` 之一
    :param message: 面向使用者的说明
    :param next_start: 续跑起点（下次请求的 ``marker``）
    :param http_status: 原始 HTTP 状态码，0 表示未获取到
    """

    def __init__(self, errno: int, message: str, next_start: str = "", http_status: int = 0) -> None:
        super().__init__(message)
        self.errno = errno
        self.message = message
        self.next_start = next_start
        self.http_status = http_status

    def continue_on_error(self) -> bool:
        """该错误是否允许从 ``next_start`` 原地续跑。"""
        return self.errno < _CONTINUABLE_BELOW

    def __str__(self) -> str:  # pragma: no cover - 仅用于日志
        start = self.next_start or "/"
        return f"errno: {self.errno}, msg: {self.message}, next_start: {start}"


# ---------------------------------------------------------------------------
# HTTP 状态码统计
# ---------------------------------------------------------------------------


class HttpStatusTracker:
    """线程安全的 HTTP 状态码计数器，用于运行期观测。"""

    def __init__(self) -> None:
        import threading

        self._lock = threading.Lock()
        self._map: dict[int, int] = {}

    def inc(self, code: int) -> None:
        if not code:
            return
        with self._lock:
            self._map[code] = self._map.get(code, 0) + 1

    def snapshot(self) -> list[tuple[int, int]]:
        with self._lock:
            return sorted(self._map.items())

    def __str__(self) -> str:
        return ", ".join(f"[{code} => {count}]" for code, count in self.snapshot())


# ---------------------------------------------------------------------------
# ETag 解析
# ---------------------------------------------------------------------------

_ETAG_MD5_RE = re.compile(r"^[0-9a-fA-F]{32}$")


def parse_etag(raw: Optional[str]) -> tuple[Optional[bytes], int]:
    """解析对象存储返回的 ETag。

    支持两种形态：

    * ``"d41d8cd98f00b204e9800998ecf8427e"`` —— 单段 MD5；
    * ``"c8af37b371ec442ad415feeb87d83246-186"`` —— 分段上传的 MD5 与段数。

    返回 ``(md5_bytes | None, parts)``。无法识别时返回 ``(None, 0)``，
    由调用方按「ETag 不可用」处理——这比中断整次盘点更稳妥。
    """
    if not raw:
        return None, 0

    value = raw.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    if not value:
        return None, 0

    if "-" in value:
        digest, _, suffix = value.rpartition("-")
        if _ETAG_MD5_RE.match(digest) and suffix.isdigit():
            return bytes.fromhex(digest), int(suffix)
        return None, 0

    if _ETAG_MD5_RE.match(value):
        return bytes.fromhex(value), 0

    return None, 0


# ---------------------------------------------------------------------------
# 对象元数据
# ---------------------------------------------------------------------------


@dataclass
class ObjectProps:
    """单个对象的元数据及其在比对过程中的状态。

    :param size: 对象大小（字节）
    :param last_modified: 最后修改时间（Unix 秒）
    :param etag_md5: ETag 的 MD5 摘要，``None`` 表示不可用
    :param etag_parts: 分段上传的段数，0 表示未分段
    """

    size: int = 0
    last_modified: int = 0
    etag_md5: Optional[bytes] = None
    etag_parts: int = 0
    flags: int = 0
    status: int = STATUS_OPEN

    # -- 侧别 ---------------------------------------------------------------

    def set_side(self, side: int) -> None:
        self.flags |= side

    def is_left(self) -> bool:
        return (self.flags & FLAG_DIR_LEFT) == FLAG_DIR_LEFT

    def is_right(self) -> bool:
        return (self.flags & FLAG_DIR_RIGHT) == FLAG_DIR_RIGHT

    def is_both(self) -> bool:
        return (self.flags & FLAG_DIR_BOTH) == FLAG_DIR_BOTH

    def is_diff_mode(self) -> bool:
        return (self.flags & FLAG_DIFF_MODE) == FLAG_DIFF_MODE

    # -- ETag ---------------------------------------------------------------

    def is_etag_missing(self) -> bool:
        """ETag 是否缺失（无法参与内容一致性判定）。"""
        return self.etag_md5 is None

    def etag_string(self) -> str:
        """还原为十六进制 ETag 字面量；缺失时返回空串。"""
        if self.etag_md5 is None:
            return ""
        digest = self.etag_md5.hex()
        return digest if self.etag_parts == 0 else f"{digest}-{self.etag_parts}"

    def etag_key(self) -> tuple[Optional[bytes], int]:
        return self.etag_md5, self.etag_parts

    # -- 结果 ---------------------------------------------------------------

    def final_status_check(self, matches_filter=None) -> MatchResult:
        """在导出阶段给出该对象的最终结论。

        :param matches_filter: 由 :mod:`obsfast.filter_expr` 提供的判定回调，
            仅单桶模式生效；返回 ``False`` 表示对象被过滤条件排除。
        """
        if not self.is_diff_mode() and matches_filter is not None:
            if not matches_filter(self):
                return MatchResult.IGNORE

        if self.status == STATUS_FILTER_OUT:
            return MatchResult.IGNORE

        if self.status in (
            STATUS_SIZE_NOT_MATCH,
            STATUS_ETAG_NOT_AVAIL,
            STATUS_ETAG_NOT_MATCH,
        ):
            return MatchResult.ASTRISK

        if self.status == STATUS_MATCH:
            return MatchResult.EQUAL

        if self.status == STATUS_OPEN:
            if self.is_both():
                raise FastListError(
                    ERR_UNKNOWN,
                    f"未收敛的对象状态 flags={self.flags} status={self.status}",
                )
            if self.is_left():
                return MatchResult.PLUS
            if self.is_right():
                return MatchResult.MINUS
            raise FastListError(
                ERR_UNKNOWN,
                f"侧别标志缺失 flags={self.flags} status={self.status}",
            )

        raise FastListError(
            ERR_UNKNOWN, f"无法识别的对象状态 flags={self.flags} status={self.status}"
        )

    def match(
        self,
        other: "ObjectProps",
        matches_filter=None,
        compare_etag: bool = True,
    ) -> MatchResult:
        """与另一侧的同名对象完成比对，并把结论写回 ``self``。

        同一对象可能因重试被重复采集，重试结果会覆盖旧值而非参与比对。

        :param matches_filter: 比对模式下的过滤回调，返回 ``False`` 表示整体忽略
        :param compare_etag: 是否用内容标识判定一致性。

            该开关存在的原因是服务端行为差异：当桶使用 ``kms`` 默认加密时，
            列举结果中的 ``ETag`` **不等于**对象内容的 MD5，且同一内容重复上传
            会得到不同的取值（已实测确认）。此时若仍按 ``ETag`` 判定，会把全部
            对象误判为「两侧不一致」。因此检测到该情形时应关闭内容标识比对，
            退化为「仅按大小判定」，并在输出中明确标注结论口径。
        """
        if self.is_both():
            return MatchResult.DUP

        # 同一侧的重复采集：直接用最新结果覆盖
        if (self.is_right() and other.is_right()) or (self.is_left() and other.is_left()):
            self.size = other.size
            self.last_modified = other.last_modified
            self.etag_md5 = other.etag_md5
            self.etag_parts = other.etag_parts
            self.status = other.status
            return MatchResult.DUP

        left, right = (self, other) if self.is_left() else (other, self)

        if matches_filter is not None:
            if not matches_filter(left, right):
                self._copy_from(left)
                self.flags |= FLAG_DIR_BOTH
                self.status = STATUS_FILTER_OUT
                return MatchResult.IGNORE

        if left.size != right.size:
            self._copy_from(left)
            self.flags |= FLAG_DIR_BOTH
            self.status = STATUS_SIZE_NOT_MATCH
            return MatchResult.ASTRISK

        if not compare_etag:
            # 内容标识不可比：大小一致即视为一致，内容是否相同不作结论
            self._copy_from(left)
            self.flags |= FLAG_DIR_BOTH
            self.status = STATUS_MATCH
            return MatchResult.EQUAL

        if left.is_etag_missing() or right.is_etag_missing():
            self._copy_from(left)
            self.flags |= FLAG_DIR_BOTH
            self.status = STATUS_ETAG_NOT_AVAIL
            return MatchResult.ASTRISK

        if left.etag_key() != right.etag_key():
            self._copy_from(left)
            self.flags |= FLAG_DIR_BOTH
            self.status = STATUS_ETAG_NOT_MATCH
            return MatchResult.ASTRISK

        self._copy_from(left)
        self.flags |= FLAG_DIR_BOTH
        self.status = STATUS_MATCH
        return MatchResult.EQUAL

    def _copy_from(self, other: "ObjectProps") -> None:
        self.size = other.size
        self.last_modified = other.last_modified
        self.etag_md5 = other.etag_md5
        self.etag_parts = other.etag_parts

    # -- 输出 ---------------------------------------------------------------

    def to_row(self) -> tuple[int, int, str]:
        """转换为 parquet 行（``Size`` / ``LastModified`` / ``ETag``）。

        ``Key`` 与 ``DiffFlag`` 由导出层补齐。
        """
        return (
            self.size,
            self.last_modified,
            self.etag_string(),
        )


def props_from_object(obj) -> ObjectProps:
    """由列举结果条目构造 :class:`ObjectProps`。

    :param obj: 列举响应中的单个对象条目。既可以是直接携带
        ``lastModified`` 时间戳的对象，也可以由访问层提供 ``last_modified``
        （Unix 秒）字段；两种形态都需支持，因为访问层已把服务端混用的
        ISO 8601 / RFC 1123 时间表示统一归一为 Unix 秒。
    """
    md5, parts = parse_etag(getattr(obj, "etag", None))
    size = getattr(obj, "size", 0) or 0
    modified = getattr(obj, "lastModified", None)
    if modified is None:
        modified = getattr(obj, "last_modified", None)
    epoch = 0
    if isinstance(modified, bool):  # bool 是 int 的子类，需先排除
        epoch = 0
    elif isinstance(modified, (int, float)):
        epoch = int(modified)
    elif modified is not None:
        try:
            epoch = int(modified.timestamp())
        except (AttributeError, ValueError, OSError, OverflowError):
            epoch = 0
    return ObjectProps(size=int(size), last_modified=epoch, etag_md5=md5, etag_parts=parts)
