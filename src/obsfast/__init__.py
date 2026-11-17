"""obsfast —— 对象存储桶的并发盘点与差异比对工具。

模块划分::

    config          运行配置与输出路径推导
    model           对象元数据、匹配语义、错误分类
    filter_expr     受控的对象过滤表达式
    obs_client      签名、请求与列举
    listing         并发盘点任务
    data_map        键空间切分与前缀聚合
    inventory       桶清单配置与清单报告转换
    parquet_writer  对象元数据导出
    monitor         运行期观测
    cli             命令行入口

对外主要入口是 :func:`obsfast.cli.main`，安装后通过 ``obsfast`` 命令调用。
"""

__version__ = "1.0.0"
__all__ = ["__version__"]
