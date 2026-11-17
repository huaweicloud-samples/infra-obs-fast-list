"""对象存储访问层——请求签名、列举、以及供桶清单使用的原始 REST 调用。

设计要点
--------
1. **单一签名实现**。签名访问与匿名访问走同一套代码路径，只是后者跳过
   ``Authorization`` 头。这样两种模式下的分页、超时与错误分类行为完全一致，
   不会出现「加了凭据反而行为不同」的问题。
2. **只依赖标准库**。对象存储的列举、HEAD、以及桶清单配置都通过
   ``http.client`` / ``urllib`` 发起；借助 ``xml.etree`` 解析响应。这样避免了
   与官方 SDK 的版本漂移，也让签名逻辑可以被完整审阅。
3. **错误可分类**。所有失败都会转换为 :class:`obsfast.model.FastListError`，
   并带上 ``errno`` 与续跑起点，供上层决定「原地重试」还是「终止」。
"""

from __future__ import annotations

import base64
import email.utils
import hashlib
import hmac
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .config import Credentials
from .model import (
    ERR_ACCESS_DENIED,
    ERR_CLIENT_CONNECTION_TIMEOUT,
    ERR_CLIENT_GENERIC,
    ERR_NO_BUCKET,
    ERR_PERMANENT_REDIRECT,
    ERR_UNKNOWN,
    FastListError,
    ObjectProps,
    props_from_object,
)

#: 列举接口单页上限（对象存储服务约定为 1000）
MAX_KEYS_PER_PAGE = 1000
#: 单次 HTTP 请求超时（秒）
DEFAULT_REQUEST_TIMEOUT = 30

#: 参与规范化资源串的子资源名（按服务端定义枚举，缺省为空）
_SUB_RESOURCES = frozenset(
    {
        "acl",
        "append",
        "backtosave",
        "backup",
        "cdn",
        "cors",
        "customdomain",
        "delete",
        "deletebucket",
        "directcoldaccess",
        "encryption",
        "fetch",
        "inventory",
        "lifecycle",
        "location",
        "logging",
        "loggingstatus",
        "metadata",
        "notification",
        "objectlock",
        "objecttype",
        "partNumber",
        "parts",
        "policy",
        "position",
        "quota",
        "rename",
        "replication",
        "requestPayment",
        "restore",
        "tagging",
        "torrent",
        "uploadId",
        "uploads",
        "versionId",
        "versioning",
        "versions",
        "website",
    }
)

_ERRNO_BY_STATUS = {
    301: ERR_PERMANENT_REDIRECT,
    403: ERR_ACCESS_DENIED,
    404: ERR_NO_BUCKET,
}


# ---------------------------------------------------------------------------
# 签名
# ---------------------------------------------------------------------------


def _canonicalized_headers(headers: dict[str, str]) -> str:
    """把 ``x-obs-*`` 头按名称排序后拼成规范化头串。"""
    picked = sorted(
        (name.lower(), " ".join(str(value).strip().split()))
        for name, value in headers.items()
        if name.lower().startswith("x-obs-")
    )
    return "".join(f"{name}:{value}\n" for name, value in picked)


def _canonicalized_resource(bucket: str, key: str, query: dict[str, list[str]]) -> str:
    """拼装规范化资源串 ``/{bucket}/{key}?{子资源}``。

    子资源按名称字典序排序；仅有名称无取值的子资源只输出名称本身。
    """
    resource = f"/{bucket}/"
    if key:
        resource += key if key.startswith("/") else key

    sub_items: list[tuple[str, Optional[str]]] = []
    for name, values in query.items():
        if name not in _SUB_RESOURCES:
            continue
        if not values:
            sub_items.append((name, None))
            continue
        for value in values:
            sub_items.append((name, value))

    if not sub_items:
        return resource

    sub_items.sort(key=lambda item: (item[0], item[1] or ""))
    parts = [
        name if value is None or value == "" else f"{name}={value}"
        for name, value in sub_items
    ]
    return f"{resource}?{'&'.join(parts)}"


def build_authorization(
    credentials: Credentials,
    method: str,
    bucket: str,
    key: str,
    query: dict[str, list[str]],
    headers: dict[str, str],
    content_md5: str = "",
    content_type: str = "",
    date_header: str = "",
) -> str:
    """按对象存储服务的头签名规范生成 ``Authorization`` 取值。

    ``StringToSign`` 由「请求方法 / Content-MD5 / Content-Type / Date /
    规范化头 / 规范化资源」六段以换行拼接而成，签名算法为 ``HMAC-SHA1``。
    """
    string_to_sign = "\n".join(
        [
            method.upper(),
            content_md5,
            content_type,
            date_header,
            _canonicalized_headers(headers)
            + _canonicalized_resource(bucket, key, query),
        ]
    )
    digest = hmac.new(
        credentials.secret_key.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    signature = base64.b64encode(digest).decode("ascii")
    return f"OBS {credentials.access_key}:{signature}"


# ---------------------------------------------------------------------------
# 响应模型
# ---------------------------------------------------------------------------


@dataclass
class ListPage:
    """一页列举结果。"""

    items: list[tuple[str, ObjectProps]] = field(default_factory=list)
    is_truncated: bool = False
    next_marker: str = ""
    key_count: int = 0


@dataclass
class RawResponse:
    """原始 REST 响应。"""

    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def json(self) -> dict:
        import json

        if not self.body:
            return {}
        return json.loads(self.body.decode("utf-8"))

    def error_code(self) -> tuple[str, str]:
        """从错误响应体中提取 ``(Code, Message)``。"""
        if not self.body:
            return "", ""
        try:
            root = ET.fromstring(self.body)
        except ET.ParseError:
            return "", self.body[:200].decode("utf-8", "replace")
        # 与其余解析保持一致：按本地名查找，不依赖服务端使用的命名空间取值
        code = _text(root, "Code")
        message = _text(root, "Message")
        return code, message


# ---------------------------------------------------------------------------
# 客户端
# ---------------------------------------------------------------------------


class ObsEndpoint:
    """对象存储端点客户端。

    :param region: 区域标识，用于推导默认端点与日志标注
    :param endpoint: 服务端点，形如 ``https://obs.cn-north-4.myhuaweicloud.com``
    :param credentials: 访问凭据；``None`` 表示匿名访问
    :param path_style: 是否使用路径风格寻址（默认关闭，虚拟主机风格为服务端要求）
    :param timeout: 单次请求超时（秒）
    """

    def __init__(
        self,
        region: str,
        endpoint: str,
        credentials: Optional[Credentials] = None,
        path_style: bool = False,
        timeout: int = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        parsed = urllib.parse.urlsplit(endpoint if "://" in endpoint else f"https://{endpoint}")
        self.region = region
        self.scheme = parsed.scheme or "https"
        self.host = parsed.netloc
        self.base_path = parsed.path.rstrip("/")
        self.credentials = credentials
        self.path_style = path_style
        self.timeout = timeout
        self._ssl_context = ssl.create_default_context()

    # -- 请求 ---------------------------------------------------------------

    @property
    def anonymous(self) -> bool:
        return self.credentials is None

    def _host_header(self, bucket: str) -> tuple[str, str]:
        """返回 ``(Host 头, URL 前缀)``，按寻址风格决定桶名位置。"""
        if self.path_style:
            return self.host, f"{self.scheme}://{self.host}{self.base_path}/{bucket}"
        return f"{bucket}.{self.host}", f"{self.scheme}://{bucket}.{self.host}{self.base_path}"

    def request(
        self,
        method: str,
        bucket: str,
        key: str = "",
        query: Optional[dict[str, list[str]]] = None,
        body: bytes = b"",
        content_type: str = "",
        extra_headers: Optional[dict[str, str]] = None,
    ) -> RawResponse:
        """发起一次 REST 请求并返回响应。

        网络层异常统一转换为可续跑的 :class:`FastListError`；HTTP 层错误则由
        :meth:`raise_for_status` 视调用方需要决定是否抛出。
        """
        query = query or {}
        host_header, url_prefix = self._host_header(bucket)
        resource_key = "" if self.path_style else key

        path = urllib.parse.quote(key, safe="/~!*'()") if key else ""
        # 根资源在路径风格下以 "/" 结尾；虚拟主机风格下根资源不带尾斜杠
        if self.path_style and not path:
            url = url_prefix + "/"
        elif self.path_style:
            url = f"{url_prefix}/{path}"
        else:
            url = f"{url_prefix}/{path}" if path else url_prefix + "/"

        encoded_query = urllib.parse.urlencode(
            [(name, value) for name, values in query.items() for value in values]
        )
        if encoded_query:
            url = f"{url}?{encoded_query}"

        headers: dict[str, str] = {
            "Host": host_header,
            "Date": email.utils.formatdate(usegmt=True),
        }
        if content_type:
            headers["Content-Type"] = content_type
        if body:
            headers["Content-MD5"] = base64.b64encode(
                hashlib.md5(body).digest()
            ).decode("ascii")
        if extra_headers:
            headers.update(extra_headers)

        if self.credentials is not None:
            headers["Authorization"] = build_authorization(
                self.credentials,
                method,
                bucket,
                resource_key,
                query,
                headers,
                content_md5=headers.get("Content-MD5", ""),
                content_type=content_type,
                date_header=headers["Date"],
            )

        request = urllib.request.Request(url, data=body or None, method=method.upper())
        for name, value in headers.items():
            request.add_header(name, value)

        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout, context=self._ssl_context
            ) as response:
                return RawResponse(
                    status=response.status,
                    headers={k.lower(): v for k, v in response.headers.items()},
                    body=response.read(),
                )
        except urllib.error.HTTPError as exc:
            return RawResponse(
                status=exc.code,
                headers={k.lower(): v for k, v in (exc.headers or {}).items()},
                body=exc.read() or b"",
            )
        except (socket.timeout, TimeoutError) as exc:
            raise FastListError(
                ERR_CLIENT_CONNECTION_TIMEOUT, f"连接超时: {exc}", http_status=0
            ) from exc
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (socket.timeout, TimeoutError)):
                raise FastListError(
                    ERR_CLIENT_CONNECTION_TIMEOUT, f"连接超时: {reason}", http_status=0
                ) from exc
            raise FastListError(
                ERR_CLIENT_GENERIC, f"无法连接 {self.host}: {reason}", http_status=0
            ) from exc
        except ssl.SSLError as exc:
            raise FastListError(
                ERR_CLIENT_GENERIC, f"传输层握手失败: {exc}", http_status=0
            ) from exc
        except OSError as exc:
            raise FastListError(
                ERR_CLIENT_GENERIC, f"网络异常: {exc}", http_status=0
            ) from exc

    @staticmethod
    def raise_for_status(response: RawResponse, context: str) -> None:
        """把非 2xx 响应转换为带分类的错误。"""
        if response.ok:
            return
        code, message = response.error_code()
        errno = _ERRNO_BY_STATUS.get(response.status, ERR_UNKNOWN)
        if code == "NoSuchBucket":
            errno = ERR_NO_BUCKET
        elif code in ("AccessDenied", "SignatureDoesNotMatch"):
            errno = ERR_ACCESS_DENIED
        elif code == "PermanentRedirect":
            errno = ERR_PERMANENT_REDIRECT
        detail = f"{code}: {message}" if code else message or "无响应体"
        raise FastListError(
            errno,
            f"{context} 失败（HTTP {response.status}）{detail}",
            http_status=response.status,
        )

    # -- 桶操作 -------------------------------------------------------------

    def head_bucket(self, bucket: str) -> bool:
        """判断桶是否存在且可访问。"""
        response = self.request("HEAD", bucket)
        if response.status in (200, 204):
            return True
        if response.status == 404:
            return False
        self.raise_for_status(response, f"探测桶 {bucket}")
        return True

    def bucket_encryption_algorithm(self, bucket: str) -> Optional[str]:
        """读取桶的默认服务端加密算法。

        返回 ``None`` 表示未配置默认加密；常见取值 ``kms`` / ``AES256``。

        该信息之所以重要：使用 ``kms`` 算法时，列举结果中的 ``ETag`` 不再等于
        对象内容的 MD5（且每次上传都会变化），因此不能用于判断内容是否一致。
        详见 :func:`obsfast.model.ObjectProps.match` 中 ``compare_etag`` 的说明。
        """
        response = self.request("GET", bucket, query={"encryption": [""]})
        if response.status == 404:
            return None
        if not response.ok:
            # 无权读取加密配置时不影响盘点，按「未知」处理
            logger.debug("读取 %s 的加密配置失败：HTTP %s", bucket, response.status)
            return None
        if not response.body:
            return None
        try:
            root = ET.fromstring(response.body)
        except ET.ParseError:
            return None
        for node in root.iter():
            if _local_name(node.tag) == "SSEAlgorithm":
                return (node.text or "").strip() or None
        return None

    def supports_content_etag(self, bucket: str) -> bool:
        """桶上的 ETag 是否可作为内容摘要（即未使用 KMS 默认加密）。"""
        algorithm = self.bucket_encryption_algorithm(bucket)
        return (algorithm or "").lower() != "kms"

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        marker: str = "",
        max_keys: int = MAX_KEYS_PER_PAGE,
    ) -> ListPage:
        """列举对象（不分层，``delimiter`` 置空以获得扁平结果）。

        :param marker: 起始位置，**独占**——返回结果中不含该键本身
        """
        query: dict[str, list[str]] = {"max-keys": [str(max_keys)]}
        if prefix:
            query["prefix"] = [prefix]
        if marker:
            query["marker"] = [marker]

        response = self.request("GET", bucket, query=query)
        self.raise_for_status(response, f"列举 {bucket}")
        return _parse_list_result(response.body)

    def iter_objects(self, bucket: str, prefix: str = ""):
        """按需迭代桶内全部对象（分页对调用方透明）。"""
        marker = ""
        while True:
            page = self.list_objects(bucket, prefix=prefix, marker=marker)
            for item in page.items:
                yield item
            if not page.is_truncated:
                return
            marker = page.next_marker or (page.items[-1][0] if page.items else "")

    def get_object(self, bucket: str, key: str) -> bytes:
        """读取单个对象的完整内容。"""
        response = self.request("GET", bucket, key=key)
        self.raise_for_status(response, f"读取对象 {bucket}/{key}")
        return response.body

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """写入单个对象（用于准备自验数据与清单夹具）。"""
        response = self.request(
            "PUT", bucket, key=key, body=body, content_type=content_type
        )
        self.raise_for_status(response, f"写入对象 {bucket}/{key}")

    def delete_object(self, bucket: str, key: str) -> None:
        """删除单个对象。"""
        response = self.request("DELETE", bucket, key=key)
        if response.status not in (204, 404):
            self.raise_for_status(response, f"删除对象 {bucket}/{key}")


# ---------------------------------------------------------------------------
# 响应解析
# ---------------------------------------------------------------------------


def _local_name(tag: str) -> str:
    """去掉命名空间前缀，只保留标签本地名。"""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _find(element: ET.Element, name: str) -> Optional[ET.Element]:
    """按本地名查找子节点，不依赖服务端使用的命名空间取值。"""
    for child in element:
        if _local_name(child.tag) == name:
            return child
    return None


def _text(element: ET.Element, name: str, default: str = "") -> str:
    found = _find(element, name)
    if found is None or found.text is None:
        return default
    return found.text


def _iter_children(element: ET.Element, name: str):
    for child in element:
        if _local_name(child.tag) == name:
            yield child


def _parse_last_modified(value: str) -> int:
    """把响应中的时间字段解析为 Unix 秒。

    服务端在同一响应族里会混用两种时间表示——列举结果中的 ``LastModified``
    是 ISO 8601（形如 ``2026-10-15T03:46:45.555Z``），而部分头字段是 RFC 1123。
    两种都需支持，任一失败都退化为 0 而不是中断盘点。
    """
    if not value:
        return 0
    text = value.strip().replace("Z", "+00:00")
    # ISO 8601（含毫秒）
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
    if parsed is None:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return 0
    if parsed is None:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


class _RawObject:
    """把 XML 节点适配为 :func:`props_from_object` 可消费的对象。"""

    __slots__ = ("etag", "size", "lastModified")

    def __init__(self, element: ET.Element) -> None:
        etag = _text(element, "ETag")
        self.etag = etag or None
        try:
            self.size = int(_text(element, "Size", "0") or 0)
        except ValueError:
            self.size = 0
        self.lastModified = _parse_last_modified(_text(element, "LastModified"))


def _parse_list_result(body: bytes) -> ListPage:
    if not body:
        return ListPage()
    root = ET.fromstring(body)
    page = ListPage()
    for element in _iter_children(root, "Contents"):
        key = _text(element, "Key")
        if not key:
            continue
        page.items.append((key, props_from_object(_RawObject(element))))
    page.key_count = len(page.items)
    page.is_truncated = _text(root, "IsTruncated", "false").lower() == "true"
    page.next_marker = _text(root, "NextMarker")
    if page.is_truncated and not page.next_marker and page.items:
        page.next_marker = page.items[-1][0]
    return page


# ---------------------------------------------------------------------------
# 工厂
# ---------------------------------------------------------------------------


def create_endpoint(
    region: str,
    endpoint: Optional[str],
    credentials: Optional[Credentials],
    path_style: bool = False,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> ObsEndpoint:
    """按配置创建端点客户端。

    ``path_style`` 默认关闭——服务端要求使用虚拟主机风格寻址；仅在对接
    对象存储兼容网关等自建端点时才需要开启。
    """
    from .config import default_endpoint

    return ObsEndpoint(
        region=region,
        endpoint=endpoint or default_endpoint(region),
        credentials=credentials,
        path_style=path_style,
        timeout=timeout,
    )


def wait_for_bucket(endpoint: ObsEndpoint, bucket: str, attempts: int = 3) -> None:
    """确认桶可用；连续失败时抛出最后一次的分类错误。"""
    last: Optional[FastListError] = None
    for attempt in range(attempts):
        try:
            if endpoint.head_bucket(bucket):
                return
            raise FastListError(ERR_NO_BUCKET, f"桶 {bucket} 不存在或不可访问")
        except FastListError as exc:
            last = exc
            if not exc.continue_on_error():
                raise
            if attempt + 1 < attempts:
                time.sleep(1 + attempt)
    if last is not None:
        raise last
