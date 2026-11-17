"""运行配置——命令行参数解析结果与派生的输出路径。

本模块把命令行输入整理为一个不可变的 :class:`RunConfig`，集中计算：

* 默认的键空间提示文件路径（``{region}_{bucket}_ks_hints.input``）；
* 默认的输出路径（``.ks`` 前缀分布文件、``.parquet`` 元数据文件、日志文件）；
* 端点与寻址方式（虚拟主机风格 / 路径风格）的最终取值。

把这些规则集中在一处，可以避免各子命令之间出现不一致的默认值。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from .model import RunMode

#: 未指定区域时使用的默认区域（华北-北京四）
DEFAULT_REGION = "cn-north-4"
#: 对象存储服务的默认服务端点模板
DEFAULT_ENDPOINT_TEMPLATE = "https://obs.{region}.myhuaweicloud.com"

#: 运行模式到侧别目录后缀的映射，用于拼接默认文件名
_MODE_TAG = {
    RunMode.LIST: "list",
    RunMode.BIDIR: "diff",
}

_UNSAFE_FILENAME_CHARS = re.compile(r"[^0-9A-Za-z._\-\u4e00-\u9fff]+")


def sanitize_component(text: str) -> str:
    """把任意字符串整理为可安全用于文件名的片段。"""
    cleaned = _UNSAFE_FILENAME_CHARS.sub("_", text.strip())
    return cleaned.strip("_") or "unnamed"


def timestamp_tag(moment: datetime | None = None) -> str:
    """生成 ``YYYYMMDDThhmmssZ`` 形式的时间标签（UTC）。"""
    moment = moment or datetime.now(timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def default_endpoint(region: str) -> str:
    """按区域推导对象存储服务端点。"""
    return DEFAULT_ENDPOINT_TEMPLATE.format(region=region)


@dataclass(frozen=True)
class Credentials:
    """访问凭据。

    ``access_key`` / ``secret_key`` 只保存在内存中，任何情况下都不写入日志、
    报告或产物文件。
    """

    access_key: str
    secret_key: str
    security_token: str | None = None

    @classmethod
    def from_env(cls) -> "Credentials | None":
        """从环境变量读取凭据；变量缺失时返回 ``None``（交由匿名访问路径处理）。"""
        ak = os.environ.get("HW_ACCESS_KEY") or os.environ.get("OBS_ACCESS_KEY_ID")
        sk = os.environ.get("HW_SECRET_KEY") or os.environ.get("OBS_SECRET_ACCESS_KEY")
        token = os.environ.get("HW_SECURITY_TOKEN") or os.environ.get("OBS_SECURITY_TOKEN")
        if ak and sk:
            return cls(access_key=ak, secret_key=sk, security_token=token)
        return None

    def __repr__(self) -> str:  # pragma: no cover - 防泄漏
        return "Credentials(access_key=***, secret_key=***)"


@dataclass(frozen=True)
class RunConfig:
    """一次运行的完整配置。"""

    mode: RunMode
    prefix: str
    threads: int
    concurrency: int
    bucket: str
    region: str
    target_bucket: str | None = None
    target_region: str | None = None
    endpoint: str | None = None
    target_endpoint: str | None = None
    path_style: bool = False
    anonymous: bool = False
    filter_expr: str | None = None
    ks_hints_file: str | None = None
    output_ks_file: str | None = None
    output_parquet_file: str | None = None
    output_log_file: str | None = None
    include_equal: bool = False
    compare_etag: bool = True
    timestamp: str = ""

    # -- 派生量 -------------------------------------------------------------

    @property
    def effective_endpoint(self) -> str:
        return self.endpoint or default_endpoint(self.region)

    @property
    def effective_target_endpoint(self) -> str:
        if self.mode is not RunMode.BIDIR:
            return self.effective_endpoint
        return self.target_endpoint or default_endpoint(self.target_region or self.region)

    @property
    def region_prefix(self) -> str:
        return f"{sanitize_component(self.region)}_" if self.region else ""

    @property
    def target_region_prefix(self) -> str:
        if self.mode is not RunMode.BIDIR:
            return ""
        region = self.target_region or ""
        return f"{sanitize_component(region)}_" if region else ""

    @property
    def mode_tag(self) -> str:
        return _MODE_TAG[self.mode]

    # -- 默认路径 -----------------------------------------------------------

    @property
    def ks_hints_path(self) -> str:
        """键空间提示文件路径（输入侧）。"""
        if self.ks_hints_file:
            return self.ks_hints_file
        return f"{self.region_prefix}{self.bucket}_ks_hints.input"

    @property
    def ks_output_path(self) -> str:
        """前缀分布文件路径（输出侧）。"""
        if self.output_ks_file:
            return self.output_ks_file
        return f"{self.region_prefix}{self.bucket}_{self.timestamp}.ks"

    @property
    def parquet_output_path(self) -> str:
        """对象元数据文件路径（输出侧）。"""
        if self.output_parquet_file:
            return self.output_parquet_file
        if self.mode is RunMode.LIST:
            return f"{self.region_prefix}{self.bucket}_{self.timestamp}.parquet"
        target = self.target_bucket or "target"
        return (
            f"{self.region_prefix}{self.bucket}_"
            f"{self.target_region_prefix}{target}_{self.timestamp}.parquet"
        )

    @property
    def log_path(self) -> str | None:
        """日志文件路径；未开启文件日志时返回 ``None``。"""
        if self.output_log_file:
            return self.output_log_file
        return f"obsfast_{self.mode_tag}_{self.timestamp}.log"

    @property
    def include_equal_flag(self) -> bool:
        """比对模式下是否导出「两侧一致」的对象（默认不导出）。"""
        return self.include_equal


def build_config(
    *,
    mode: RunMode,
    bucket: str,
    region: str | None = None,
    prefix: str | None = None,
    threads: int = 10,
    concurrency: int = 100,
    target_bucket: str | None = None,
    target_region: str | None = None,
    endpoint: str | None = None,
    target_endpoint: str | None = None,
    path_style: bool | None = None,
    anonymous: bool = False,
    filter_expr: str | None = None,
    ks_hints_file: str | None = None,
    output_ks_file: str | None = None,
    output_parquet_file: str | None = None,
    output_log_file: str | None = None,
    include_equal: bool = False,
    compare_etag: bool = True,
    timestamp: str | None = None,
) -> RunConfig:
    """把命令行参数整理为 :class:`RunConfig`。

    ``prefix`` 缺省或写作 ``/`` 时统一归一为空串——空串表示「从桶内第一个
    对象开始」，语义上等同于根前缀，但不参与后续的字典序比较。
    """
    normalized_prefix = "" if prefix in (None, "", "/") else prefix
    resolved_region = region or os.environ.get("HW_REGION") or DEFAULT_REGION
    # 使用默认服务端点时采用虚拟主机风格寻址（对象存储服务的要求）；
    # 只有在显式传入自定义端点时才自动切换到路径风格寻址。
    resolved_path_style = path_style if path_style is not None else endpoint is not None

    return RunConfig(
        mode=mode,
        prefix=normalized_prefix,
        threads=max(1, threads),
        concurrency=max(1, concurrency),
        bucket=bucket,
        region=resolved_region,
        target_bucket=target_bucket,
        target_region=target_region or (resolved_region if target_bucket else None),
        endpoint=endpoint,
        target_endpoint=target_endpoint,
        path_style=resolved_path_style,
        anonymous=anonymous,
        filter_expr=filter_expr,
        ks_hints_file=ks_hints_file,
        output_ks_file=output_ks_file,
        output_parquet_file=output_parquet_file,
        output_log_file=output_log_file,
        include_equal=include_equal,
        compare_etag=compare_etag,
        timestamp=timestamp or timestamp_tag(),
    )
