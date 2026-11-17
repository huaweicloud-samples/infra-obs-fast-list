"""命令行入口——参数解析、任务编排与结果落盘。

子命令
------
``list``              单桶并发盘点，导出对象元数据与前缀分布
``diff``              双桶并发盘点并比对，按差异标记导出
``split``             把前缀分布文件切分为指定段数的键空间提示
``inventory``         解析桶清单报告，转换为前缀分布文件
``inventory-config``  配置 / 查看 / 删除桶清单规则

典型链路::

    # 1) 首次全量盘点，产出前缀分布
    obsfast list --bucket my-bucket --region cn-north-4

    # 2) 切成 64 段，供后续并发盘点加速
    obsfast split --ks cn-north-4_my-bucket_20260101T000000Z.ks --count 64 \\
                  --output cn-north-4_my-bucket_ks_hints.input

    # 3) 带提示文件再次盘点
    obsfast list --bucket my-bucket --region cn-north-4 \\
                 --ks-file cn-north-4_my-bucket_ks_hints.input

    # 4) 双桶差异比对
    obsfast diff --bucket src-bucket --target-bucket dst-bucket \\
                 --region cn-north-4 --target-region cn-north-4
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
from typing import Optional, Sequence

from . import __version__
from .config import Credentials, RunConfig, build_config, sanitize_component
from .data_map import KeySpaceHints, PrefixMap, load_hint_prefixes, parse_ks_line, split_prefixes
from .filter_expr import FilterSyntaxError, compile_filter
from .inventory import (
    InventoryConfiguration,
    delete_inventory,
    dump_prefix_counts,
    get_inventory,
    inventory_prefix_counts,
    list_inventories,
    parse_manifest_location,
    put_inventory,
)
from .listing import Batch, ListContext, SharedState, left_flags, right_flags, run_flat_list
from .model import FastListError, RunMode
from .monitor import Heartbeat, summarize
from .obs_client import ObsEndpoint, create_endpoint
from .parquet_writer import ParquetSink

logger = logging.getLogger("obsfast")

#: 默认提示文件后缀
KS_HINTS_SUFFIX = "_ks_hints.input"
#: 未指定区域时的兜底取值
FALLBACK_REGION = "cn-north-4"


# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------


class _Parser(argparse.ArgumentParser):
    """在参数错误时给出更贴近业务语义的提示。"""

    def error(self, message: str):  # pragma: no cover - argparse 回调
        self.print_usage(sys.stderr)
        raise SystemExit(f"参数错误：{message}")


def _common_parser() -> argparse.ArgumentParser:
    """``list`` / ``diff`` 共用的连接与输出参数。"""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--region", help="桶所在区域，未指定时读取 HW_REGION")
    parser.add_argument("-p", "--prefix", default="/", help="起始前缀 [默认: /]")
    parser.add_argument("-t", "--threads", type=int, default=10, help="工作线程数 [默认: 10]")
    parser.add_argument(
        "-c", "--concurrency", type=int, default=100, help="并发盘点任务数 [默认: 100]"
    )
    parser.add_argument(
        "-k",
        "--ks-file",
        dest="ks_file",
        help=f"键空间提示文件 [默认: {{region}}_{{桶名}}{KS_HINTS_SUFFIX}]",
    )
    parser.add_argument("-f", "--filter", help="对象过滤表达式")
    parser.add_argument("-l", "--log", action="store_true", help="把日志写入文件")
    parser.add_argument("--endpoint-url", help="自定义服务端点")
    parser.add_argument(
        "--path-style",
        dest="path_style",
        action="store_true",
        help="强制使用路径风格寻址（使用自定义端点时自动开启）",
    )
    parser.add_argument(
        "--no-sign-request",
        dest="anonymous",
        action="store_true",
        help="不签名、匿名访问公开可读的桶",
    )
    parser.add_argument("--output-log-file", help="日志文件路径（隐含 --log）")
    parser.add_argument("--output-ks-file", help="前缀分布文件输出路径")
    parser.add_argument("--output-parquet-file", help="对象元数据输出路径")
    return parser


def _connection_parser() -> argparse.ArgumentParser:
    """清单相关子命令共用的连接参数。"""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--region", help="区域，未指定时读取 HW_REGION")
    parser.add_argument("--endpoint-url", help="自定义服务端点")
    parser.add_argument(
        "--path-style",
        dest="path_style",
        action="store_true",
        help="强制使用路径风格寻址（使用自定义端点时自动开启）",
    )
    parser.add_argument(
        "--no-sign-request",
        dest="anonymous",
        action="store_true",
        help="不签名、匿名访问公开可读的桶",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="obsfast",
        description="对象存储桶的并发盘点与差异比对工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例：\n"
            "  obsfast list --bucket my-bucket --region cn-north-4\n"
            "  obsfast diff --bucket a --target-bucket b\n"
            "  obsfast split --ks out.ks --count 64 --output hints.input\n"
            "  obsfast inventory --manifest obs://dst/path/manifest.json\n"
            "  obsfast inventory-config --action set --bucket a \\\n"
            "      --id daily-report --destination-bucket b\n"
        ),
    )
    parser.add_argument(
        "-V", "--version", action="version", version=f"obsfast {__version__}"
    )

    subparsers = parser.add_subparsers(dest="command", metavar="<命令>")

    list_parser = subparsers.add_parser(
        "list", help="单桶并发盘点并导出结果", parents=[_common_parser()]
    )
    list_parser.add_argument("--bucket", required=True, help="待盘点的桶名称")

    diff_parser = subparsers.add_parser(
        "diff", help="双桶并发盘点并比对结果", parents=[_common_parser()]
    )
    diff_parser.add_argument("--bucket", required=True, help="源桶名称")
    diff_parser.add_argument("--target-bucket", required=True, help="目标桶名称")
    diff_parser.add_argument("--target-region", help="目标桶所在区域")
    diff_parser.add_argument(
        "--target-endpoint-url", help="目标桶的服务端点（与源桶不一致时使用）"
    )
    diff_parser.add_argument(
        "--include-equal",
        action="store_true",
        help="同时导出两侧一致的对象（默认不导出）",
    )
    diff_parser.add_argument(
        "--no-etag-compare",
        dest="no_etag_compare",
        action="store_true",
        help=(
            "只按对象大小判定一致性，不比较内容标识；"
            "桶使用 KMS 默认加密时工具会自动切换到该模式"
        ),
    )

    split_parser = subparsers.add_parser(
        "split", help="把前缀分布文件切分为键空间提示"
    )
    split_parser.add_argument("-k", "--ks", required=True, help="输入的前缀分布文件")
    split_parser.add_argument("-c", "--count", type=int, required=True, help="目标切分段数")
    split_parser.add_argument("-o", "--output", required=True, help="输出的键空间提示文件")

    inv_parser = subparsers.add_parser(
        "inventory", help="解析桶清单报告并转换为前缀分布", parents=[_connection_parser()]
    )
    inv_parser.add_argument(
        "-m",
        "--manifest",
        required=True,
        help="清单报告位置，形如 obs://目标桶/路径/manifest.json",
    )
    inv_parser.add_argument(
        "-k", "--ks", help="输出的前缀分布文件 [默认: {region}_{源桶名}.ks]"
    )
    inv_parser.add_argument(
        "-c", "--concurrency", type=int, default=1, help="清单文件下载并发度 [默认: 1]"
    )

    cfg_parser = subparsers.add_parser(
        "inventory-config",
        help="配置 / 查看 / 删除桶清单规则",
        parents=[_connection_parser()],
    )
    cfg_parser.add_argument(
        "--action", choices=("set", "get", "list", "delete"), default="list", help="操作类型"
    )
    cfg_parser.add_argument("--bucket", required=True, help="源桶名称")
    cfg_parser.add_argument("--id", dest="inventory_id", help="清单规则名称")
    cfg_parser.add_argument("--destination-bucket", help="保存清单文件的目标桶")
    cfg_parser.add_argument("--destination-prefix", default="", help="清单文件路径前缀")
    cfg_parser.add_argument(
        "--frequency", choices=("Daily", "Weekly"), default="Daily", help="生成频率"
    )
    cfg_parser.add_argument(
        "--versions", choices=("Current", "All"), default="Current", help="版本范围"
    )
    cfg_parser.add_argument("--filter-prefix", default="", help="仅统计该前缀下的对象")
    cfg_parser.add_argument(
        "--optional-fields",
        default="Size,LastModifiedDate,ETag,StorageClass,IsMultipartUploaded",
        help="清单中包含的可选字段，逗号分隔",
    )
    cfg_parser.add_argument("--disabled", action="store_true", help="创建后先停用该规则")

    return parser


# ---------------------------------------------------------------------------
# 日志与凭据
# ---------------------------------------------------------------------------


def configure_logging(config: RunConfig, enabled: bool) -> None:
    """按配置初始化日志输出。"""
    level = os.environ.get("OBSFAST_LOG", "INFO").upper()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-5s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handlers: list[logging.Handler] = []
    if enabled:
        path = config.log_path
        assert path is not None
        file_handler = logging.FileHandler(path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
        # 打开文件日志时，控制台只保留告警及以上，避免结果被日志淹没
        console = logging.StreamHandler(sys.stderr)
        console.setLevel(logging.WARNING)
        console.setFormatter(formatter)
        handlers.append(console)
    else:
        console = logging.StreamHandler(sys.stderr)
        console.setFormatter(formatter)
        handlers.append(console)

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(getattr(logging, level, logging.INFO))


def resolve_credentials(anonymous: bool) -> Optional[Credentials]:
    """解析凭据；匿名模式下显式返回 ``None``。"""
    if anonymous:
        return None
    return Credentials.from_env()


# ---------------------------------------------------------------------------
# 任务编排
# ---------------------------------------------------------------------------


def _make_endpoints(config: RunConfig) -> tuple[ObsEndpoint, Optional[ObsEndpoint]]:
    credentials = resolve_credentials(config.anonymous)
    source = create_endpoint(
        config.region, config.endpoint, credentials, path_style=config.path_style
    )
    if config.mode is not RunMode.BIDIR:
        return source, None
    same_region = (config.target_region or config.region) == config.region
    same_endpoint = config.target_endpoint in (None, config.endpoint)
    if same_region and same_endpoint:
        return source, source
    target = create_endpoint(
        config.target_region or config.region,
        config.target_endpoint,
        credentials,
        path_style=config.path_style,
    )
    return source, target


def _load_hints(config: RunConfig) -> list[str]:
    path = config.ks_hints_path
    if path and os.path.exists(path):
        prefixes = load_hint_prefixes(path)
        logger.info(
            "已从 %s 载入 %d 个前缀提示，展开为 %d 段区间",
            path,
            len(prefixes),
            len(prefixes) + 1,
        )
        return prefixes
    logger.info("未找到键空间提示文件 %s，将按单段区间盘点", path)
    return []


def _log_banner(config: RunConfig) -> None:
    logger.info("obsfast %s 启动", __version__)
    logger.info(
        "  模式=%s 并发=%d 线程=%d",
        config.mode.value,
        config.concurrency,
        config.threads,
    )
    if config.mode is RunMode.LIST:
        logger.info(
            "  桶=%s 区域=%s 端点=%s", config.bucket, config.region, config.effective_endpoint
        )
    else:
        logger.info(
            "  源桶=%s@%s 端点=%s",
            config.bucket,
            config.region,
            config.effective_endpoint,
        )
        logger.info(
            "  目标桶=%s@%s 端点=%s",
            config.target_bucket,
            config.target_region,
            config.effective_target_endpoint,
        )
    logger.info("  起始前缀=%s", config.prefix or "/")
    if config.filter_expr:
        logger.info("  过滤条件=%s", config.filter_expr)


def _drain_into(prefix_map: PrefixMap, batch_queue, filter_) -> int:
    """把批次队列中已就绪的数据全部吸收到聚合映射。"""
    absorbed = 0
    while True:
        try:
            batch: Optional[Batch] = batch_queue.get_nowait()
        except Exception:
            return absorbed
        if batch is None:
            continue
        prefix_map.apply(batch, filter_)
        absorbed += sum(len(items) for items in batch.values())


def _run_list(config: RunConfig) -> int:
    filter_ = compile_filter(config.filter_expr, config.mode.value)
    source, _ = _make_endpoints(config)
    shared = SharedState()
    hints = KeySpaceHints(_load_hints(config))
    prefix_map = PrefixMap()
    ctx = ListContext(
        endpoint=source,
        bucket=config.bucket,
        side_flags=left_flags(False),
        shared=shared,
        label=config.bucket,
    )

    _log_banner(config)
    with Heartbeat([ctx], {config.bucket: hints}):
        run_flat_list(ctx, config.prefix, config.concurrency, hints)
    _drain_into(prefix_map, ctx.batch_queue, filter_)

    groups = prefix_map.diff_groups(
        include_equal=config.include_equal_flag, filter_=filter_
    )
    return _finalize(config, prefix_map, groups, shared)


def resolve_compare_etag(
    config: RunConfig, source: ObsEndpoint, target: ObsEndpoint
) -> bool:
    """判定本次比对是否可以使用内容标识（``ETag``）。

    服务端在使用 ``kms`` 默认加密时不会返回内容的 MD5，且同一内容的 ``ETag``
    会随每次上传变化。若仍按 ``ETag`` 比对，会把所有对象误判为「不一致」，
    因此这里在开跑前探测两侧桶的加密配置并自动降级为按大小判定。
    """
    if not config.compare_etag:
        return False

    algorithms: dict[str, Optional[str]] = {}
    for label, endpoint, bucket in (
        ("源桶", source, config.bucket),
        ("目标桶", target, config.target_bucket or ""),
    ):
        if not bucket:
            continue
        try:
            algorithms[label] = endpoint.bucket_encryption_algorithm(bucket)
        except FastListError as exc:
            logger.warning("读取%s加密配置失败，按未加密处理：%s", label, exc)
            algorithms[label] = None

    kms_sides = [label for label, algo in algorithms.items() if (algo or "").lower() == "kms"]
    if kms_sides:
        logger.warning(
            "检测到 %s 使用 KMS 默认加密，内容标识（ETag）不等于对象内容的摘要，"
            "无法用于判断内容是否一致；本次比对自动降级为「仅按大小判定」。",
            "、".join(kms_sides),
        )
        return False
    return True


def _run_diff(config: RunConfig) -> int:
    filter_ = compile_filter(config.filter_expr, config.mode.value)
    source, target = _make_endpoints(config)
    assert target is not None
    shared = SharedState()
    base_hints = _load_hints(config)
    prefix_map = PrefixMap()

    _log_banner(config)
    compare_etag = resolve_compare_etag(config, source, target)
    if compare_etag:
        logger.info("  一致性口径：对象大小 + 内容标识（ETag）")
    else:
        logger.info("  一致性口径：仅对象大小（内容标识不可比）")

    left_ctx = ListContext(source, config.bucket, left_flags(True), shared, label=config.bucket)
    right_ctx = ListContext(
        target,
        config.target_bucket or "",
        right_flags(True),
        shared,
        label=config.target_bucket or "target",
    )
    left_hints = KeySpaceHints(base_hints)
    right_hints = KeySpaceHints(base_hints)

    errors: list[BaseException] = []

    def drive(ctx: ListContext, hints: KeySpaceHints) -> None:
        try:
            run_flat_list(ctx, config.prefix, config.concurrency, hints)
        except BaseException as exc:  # noqa: BLE001 - 汇总后统一抛出
            errors.append(exc)
        finally:
            ctx.batch_queue.put(None)

    threads = [
        threading.Thread(target=drive, args=(left_ctx, left_hints), daemon=True),
        threading.Thread(target=drive, args=(right_ctx, right_hints), daemon=True),
    ]
    with Heartbeat(
        [left_ctx, right_ctx],
        {config.bucket: left_hints, (config.target_bucket or "target"): right_hints},
    ):
        for thread in threads:
            thread.start()
        _aggregate([left_ctx, right_ctx], prefix_map, filter_, compare_etag)
        for thread in threads:
            thread.join()

    if errors:
        raise errors[0]

    groups = prefix_map.diff_groups(
        include_equal=config.include_equal_flag, filter_=filter_
    )
    return _finalize(config, prefix_map, groups, shared, compare_etag=compare_etag)


def _aggregate(
    contexts: list[ListContext],
    prefix_map: PrefixMap,
    filter_,
    compare_etag: bool = True,
) -> None:
    """双桶模式的聚合循环：两侧都投递结束标记后退出。"""
    finished = 0
    total = len(contexts)
    while finished < total:
        for ctx in contexts:
            try:
                batch: Optional[Batch] = ctx.batch_queue.get(timeout=0.5)
            except Exception:
                continue
            if batch is None:
                finished += 1
                continue
            prefix_map.apply(batch, filter_, compare_etag)


def _finalize(
    config: RunConfig,
    prefix_map: PrefixMap,
    groups,
    shared: SharedState,
    compare_etag: bool = True,
) -> int:
    """落盘结果并输出统计摘要。"""
    snapshot = shared.snapshot()
    logger.info("盘点结束：%s", prefix_map)
    logger.info(
        "  请求统计：对象 %d 个 / 分页 %d 次 / 续跑 %d / 连接超时 %d / 其他错误 %d",
        snapshot["listed_objects"],
        snapshot["listed_pages"],
        snapshot["next_stream_timeouts"],
        snapshot["client_timeouts"],
        snapshot["client_generic_errors"],
    )
    if snapshot["http_status"]:
        logger.info(
            "  HTTP 状态码分布：%s",
            ", ".join(f"[{code} => {count}]" for code, count in snapshot["http_status"]),
        )

    ks_path = config.ks_output_path
    prefix_count, object_count = prefix_map.dump_ks(ks_path)
    print(f"前缀分布文件：{ks_path}（{prefix_count} 个前缀，{object_count} 个对象）")

    parquet_path = config.parquet_output_path
    with ParquetSink(parquet_path) as sink:
        rows = sink.write_groups(groups)
        skipped = not sink.active
    if skipped:
        print(f"对象元数据文件：已跳过（未安装 PyArrow），目标路径 {parquet_path}")
    else:
        print(f"对象元数据文件：{parquet_path}（{rows} 行）")

    if config.mode is RunMode.BIDIR and not compare_etag:
        print(
            "注意：本次比对仅按对象大小判定一致性。"
            "桶使用 KMS 默认加密时内容标识不等于内容摘要，"
            "DiffFlag=0 仅表示「两侧同名且大小一致」，不代表内容逐字节相同。"
        )

    print(summarize(config.mode.value, groups, prefix_count, object_count))
    return 0


# ---------------------------------------------------------------------------
# 子命令实现
# ---------------------------------------------------------------------------


def cmd_list(args: argparse.Namespace) -> int:
    config = build_config(
        mode=RunMode.LIST,
        bucket=args.bucket,
        region=args.region,
        prefix=args.prefix,
        threads=args.threads,
        concurrency=args.concurrency,
        endpoint=args.endpoint_url,
        path_style=True if args.path_style else None,
        anonymous=args.anonymous,
        filter_expr=args.filter,
        ks_hints_file=args.ks_file,
        output_ks_file=args.output_ks_file,
        output_parquet_file=args.output_parquet_file,
        output_log_file=args.output_log_file,
    )
    configure_logging(config, args.log or args.output_log_file is not None)
    return _run_list(config)


def cmd_diff(args: argparse.Namespace) -> int:
    config = build_config(
        mode=RunMode.BIDIR,
        bucket=args.bucket,
        region=args.region,
        prefix=args.prefix,
        threads=args.threads,
        concurrency=args.concurrency,
        target_bucket=args.target_bucket,
        target_region=args.target_region,
        endpoint=args.endpoint_url,
        target_endpoint=args.target_endpoint_url,
        path_style=True if args.path_style else None,
        anonymous=args.anonymous,
        filter_expr=args.filter,
        ks_hints_file=args.ks_file,
        output_ks_file=args.output_ks_file,
        output_parquet_file=args.output_parquet_file,
        output_log_file=args.output_log_file,
        include_equal=args.include_equal,
        compare_etag=not args.no_etag_compare,
    )
    configure_logging(config, args.log or args.output_log_file is not None)
    return _run_diff(config)


def cmd_split(args: argparse.Namespace) -> int:
    if args.count <= 0:
        raise SystemExit("切分段数必须为正整数")
    counts = _read_ks_counts(args.ks)
    total = sum(counts.values())
    points = split_prefixes(counts, args.count)
    if not points:
        print(f"对象总数 {total} 不足以切成 {args.count} 段，未生成键空间提示文件。")
        return 0
    with open(args.output, "w", encoding="utf-8", newline="\n") as handle:
        for prefix in points:
            handle.write(prefix + "\n")
    print(
        f"键空间提示文件：{args.output}"
        f"（{len(points)} 个切分点，覆盖 {total} 个对象，源前缀 {len(counts)} 个）"
    )
    return 0


def _read_ks_counts(path: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            parsed = parse_ks_line(line)
            if parsed is not None:
                counts[parsed[0]] = parsed[1]
    return counts


def cmd_inventory(args: argparse.Namespace) -> int:
    region = args.region or os.environ.get("HW_REGION") or FALLBACK_REGION
    credentials = resolve_credentials(args.anonymous)
    endpoint = create_endpoint(
        region,
        args.endpoint_url,
        credentials,
        path_style=args.path_style or args.endpoint_url is not None,
    )
    bucket, _ = parse_manifest_location(args.manifest)

    def progress(done: int, total: int) -> None:
        if total > 1:
            print(f"  已处理清单文件 {done}/{total}", file=sys.stderr)

    counts, report = inventory_prefix_counts(
        endpoint, args.manifest, concurrency=args.concurrency, progress=progress
    )
    output = args.ks or (
        f"{sanitize_component(region)}_{sanitize_component(report.source_bucket)}.ks"
    )
    prefix_count, object_count = dump_prefix_counts(output, counts)
    print(f"清单报告：源桶 {report.source_bucket}，目标桶 {bucket}，数据文件 {len(report.files)} 个")
    print(f"前缀分布文件：{output}（{prefix_count} 个前缀，{object_count} 个对象）")
    return 0


def cmd_inventory_config(args: argparse.Namespace) -> int:
    region = args.region or os.environ.get("HW_REGION") or FALLBACK_REGION
    credentials = resolve_credentials(args.anonymous)
    endpoint = create_endpoint(
        region,
        args.endpoint_url,
        credentials,
        path_style=args.path_style or args.endpoint_url is not None,
    )

    if args.action == "list":
        configs = list_inventories(endpoint, args.bucket)
        if not configs:
            print(f"桶 {args.bucket} 上暂无清单规则。")
            return 0
        print(f"桶 {args.bucket} 上的清单规则（{len(configs)} 条）：")
        for config in configs:
            state = "启用" if config.enabled else "停用"
            print(
                f"  - {config.inventory_id}：{state}，{config.frequency}，"
                f"目标 {config.destination_bucket}/{config.destination_prefix or '(默认前缀)'}"
            )
        return 0

    if args.action == "get":
        if not args.inventory_id:
            raise SystemExit("读取清单规则需要指定 --id")
        config = get_inventory(endpoint, args.bucket, args.inventory_id)
        print(f"清单规则 {config.inventory_id}：")
        print(f"  启用状态：{'启用' if config.enabled else '停用'}")
        print(f"  生成频率：{config.frequency}")
        print(f"  版本范围：{config.included_versions}")
        print(f"  目标桶：{config.destination_bucket}")
        print(f"  目标前缀：{config.destination_prefix or '(默认)'}")
        print(f"  过滤前缀：{config.filter_prefix or '(全部对象)'}")
        print(f"  可选字段：{', '.join(config.optional_fields) or '(无)'}")
        return 0

    if args.action == "delete":
        if not args.inventory_id:
            raise SystemExit("删除清单规则需要指定 --id")
        delete_inventory(endpoint, args.bucket, args.inventory_id)
        print(f"已删除桶 {args.bucket} 上的清单规则 {args.inventory_id}。")
        return 0

    # action == "set"
    if not args.inventory_id:
        raise SystemExit("创建清单规则需要指定 --id")
    if not args.destination_bucket:
        raise SystemExit("创建清单规则需要指定 --destination-bucket")
    config = InventoryConfiguration(
        inventory_id=args.inventory_id,
        destination_bucket=args.destination_bucket,
        enabled=not args.disabled,
        frequency=args.frequency,
        included_versions=args.versions,
        destination_prefix=args.destination_prefix,
        filter_prefix=args.filter_prefix,
        optional_fields=tuple(
            item.strip() for item in args.optional_fields.split(",") if item.strip()
        ),
    )
    try:
        put_inventory(endpoint, args.bucket, config)
    except ValueError as exc:
        raise SystemExit(f"清单规则不合法：{exc}") from exc
    print(
        f"已在桶 {args.bucket} 上创建清单规则 {config.inventory_id}，"
        f"清单文件将写入 {config.destination_bucket}/{config.destination_prefix or '(默认前缀)'}。"
    )
    print("提示：首次清单文件在配置后约 1 小时内生成，报告文件随后 1~2 小时产出。")
    return 0


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

_HANDLERS = {
    "list": cmd_list,
    "diff": cmd_diff,
    "split": cmd_split,
    "inventory": cmd_inventory,
    "inventory-config": cmd_inventory_config,
}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 2

    try:
        return _HANDLERS[args.command](args)
    except FilterSyntaxError as exc:
        print(f"过滤表达式错误：{exc}", file=sys.stderr)
        return 2
    except FastListError as exc:
        print(f"盘点失败：{exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("已中断。", file=sys.stderr)
        return 130
    except ValueError as exc:
        print(f"参数或数据错误：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
