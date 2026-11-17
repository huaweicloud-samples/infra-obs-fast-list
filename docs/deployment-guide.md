# 部署与使用指导

本文档给出从零到可运行的完整步骤：准备环境 → 创建桶 → 安装工具 → 执行盘点 → 验证结果 → 释放资源。

## 1. 前置条件

| 项目 | 要求 |
|-|-|
| 账号权限 | 具备对象存储的桶创建、列举、读写权限，以及清单规则的读写权限 |
| Python | 3.9 及以上（工具本体仅用标准库） |
| Terraform | 1.5.0 及以上 |
| 区域 | 默认 `cn-north-4`，可按需调整 |
| 网络 | 能访问目标区域的对象存储服务端点 |

导出 Parquet 需要额外的 `pyarrow`；不导出 Parquet 时无需安装任何第三方包。

## 2. 准备凭据

工具从环境变量读取访问凭据：

```bash
export HW_ACCESS_KEY="<访问键>"
export HW_SECRET_KEY="<密钥>"
export HW_REGION="cn-north-4"
```

> 凭据只从环境变量读取，不会写入任何配置文件或输出文件。请勿把真实凭据提交进代码仓。

## 3. 创建桶

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

预期输出末尾：

```
Apply complete! Resources: 3 added, 0 changed, 0 destroyed.

Outputs:

inventory_bucket_name = "obsfast-inventory-xxxxxxxx"
source_bucket_name = "obsfast-source-xxxxxxxx"
```

### 3.1 可配置变量

| 变量 | 默认值 | 说明 |
|-|-|-|
| `region` | `cn-north-4` | 区域 |
| `name_prefix` | `obsfast` | 桶名前缀，实际桶名另附随机后缀 |
| `source_bucket_name` | 空（自动生成） | 指定源桶名，留空则由 `name_prefix` 生成 |
| `inventory_bucket_name` | 空（自动生成） | 指定清单目标桶名 |
| `enable_versioning` | `false` | 是否开启多版本 |
| `enable_encryption` | `false` | 源桶是否启用 KMS 默认加密，见下方说明 |
| `inventory_lifecycle_days` | `30` | 清单文件保留天数，到期自动清理 |
| `abort_multipart_days` | `7` | 未完成的碎片上传清理天数 |
| `bucket_quota` | `0` | 桶配额（字节），`0` 表示不限 |
| `tags` | 见文件 | 资源标签 |

### 3.2 关于 KMS 默认加密的两点影响

把 `enable_encryption` 设为 `true` 会带来两个直接影响，请在开启前确认可以接受：

1. 列举结果中的 ETag **不再是对象内容的 MD5**，且同一内容重复上传会得到不同取值。此时差异比对会自动降级为「仅按大小判定」。
2. **KMS 加密的桶不能作为清单目标桶**，配置清单规则时服务端会返回 `400 InvalidArgument`。

因此清单目标桶始终不启用 KMS 默认加密，与 `enable_encryption` 的取值无关。

## 4. 安装工具

```bash
cd src
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt    # 仅需导出 Parquet 时执行
export PYTHONPATH="$(pwd)"
```

也可以直接使用模块方式运行，无需安装：

```bash
export PYTHONPATH="<本仓库根目录>/src"
python3 -m obsfast --help
```

## 5. 执行盘点

### 5.1 单桶盘点

```bash
python3 -m obsfast list --bucket <源桶名> --region cn-north-4 \
    --output-ks-file out.ks --output-parquet-file out.parquet
```

预期输出（节选）：

```
盘点结束：prefix count 8, object count 9
  请求统计：对象 9 个 / 分页 1 次 / 续跑 0 / 连接超时 0 / 其他错误 0
对象元数据已导出至 out.parquet（9 行）
```

### 5.2 大规模桶的分段盘点

先跑一轮拿到前缀分布，再切分，然后按切分结果并发盘点：

```bash
# 第一轮：单段快速扫描，产出前缀分布
python3 -m obsfast list --bucket <源桶名> --region cn-north-4 -k first.ks

# 切分为 64 段
python3 -m obsfast split --ks first.ks --count 64 --output hints.input

# 第二轮：按提示并发盘点
python3 -m obsfast list --bucket <源桶名> --region cn-north-4 \
    --ks-file hints.input --concurrency 64 \
    --output-ks-file full.ks --output-parquet-file full.parquet
```

### 5.3 双桶差异比对

```bash
python3 -m obsfast diff --bucket <源桶名> --target-bucket <目标桶名> \
    --region cn-north-4 --concurrency 64 \
    --output-ks-file diff.ks --output-parquet-file diff.parquet
```

默认只导出差异项。加 `--include-equal` 可同时导出两侧一致的对象。

跨区域比对时补充 `--target-region` 或 `--target-endpoint-url`。

### 5.4 过滤条件

```bash
python3 -m obsfast list --bucket <源桶名> --region cn-north-4 \
    --filter "SOURCE.size >= 1024 and SOURCE.last_modified > 1735689600"
```

可用变量：`SOURCE`（`list` 与 `diff`）、`TARGET`（仅 `diff`）。可用属性：`size`、`last_modified`。引用其他变量或属性会在启动时报错。

## 6. 使用桶清单

### 6.1 配置清单规则

```bash
python3 -m obsfast inventory-config --action set \
    --bucket <源桶名> --id daily-inventory \
    --destination-bucket <目标桶名> --destination-prefix inventory-reports \
    --frequency Daily --versions Current
```

```
已在桶 <源桶名> 上创建清单规则 daily-inventory，清单文件将写入 <目标桶名>/inventory-reports。
提示：首次清单文件在配置后约 1 小时内生成，报告文件随后 1~2 小时产出。
```

查看与删除：

```bash
python3 -m obsfast inventory-config --action get  --bucket <源桶名> --id daily-inventory
python3 -m obsfast inventory-config --action list --bucket <源桶名>
python3 -m obsfast inventory-config --action delete --bucket <源桶名> --id daily-inventory
```

### 6.2 解析清单报告

```bash
python3 -m obsfast inventory \
    --manifest "obs://<目标桶名>/inventory-reports/<源桶名>/daily-inventory/<日期目录>/manifest.json" \
    --region cn-north-4 -k inv.ks -c 4
```

```
  已处理清单文件 1/2
  已处理清单文件 2/2
清单报告：源桶 <源桶名>，目标桶 <目标桶名>，数据文件 2 个
前缀分布文件：inv.ks（8 个前缀，9 个对象）
```

生成的 `.ks` 可直接作为 `split` 的输入，形成「清单 → 切分 → 并发盘点」的完整链路。

## 7. 验证结果

```bash
python3 - <<'PY'
import pyarrow.parquet as pq
table = pq.read_table("diff.parquet")
print(table.schema)
print(table.num_rows)
PY
```

预期 schema：

```
Key: string not null
Size: uint64 not null
LastModified: uint64 not null
ETag: string not null
DiffFlag: uint8 not null
```

自查要点：

* `DiffFlag` 的分布与命令行汇总一致；
* `LastModified` 为合理的 Unix 秒（非全 0）；
* 分段盘点与单段盘点的 `.ks` 结果逐行一致。

## 8. 常见问题

| 现象 | 原因与处理 |
|-|-|
| `403 VirtualHostDomainRequired` | 使用了路径风格访问区域域名。去掉 `--path-style`，或使用自定义端点 |
| `403 SignatureDoesNotMatch` | 本地时间与服务端偏差过大，或凭据有误。校准系统时间 |
| `400 InvalidArgument: The destination bucket is KMS encryption bucket` | 清单目标桶启用了 KMS 默认加密。换用未加密的桶 |
| `LastModified` 全为 0 | 时间字段解析失败，检查服务端返回的时间格式是否为支持的类型 |
| 差异比对时全部对象都被判为不一致 | 桶启用了 KMS 默认加密，ETag 不可比。确认日志中的口径提示 |
| 提示文件未生效（日志显示载入 0 个前缀） | 提示文件应为每行一个前缀，而非 CSV 行 |

## 9. 释放资源

```bash
cd infra
terraform destroy
```

> 销毁前请先删除桶内对象；非空桶会导致销毁失败。同时记得删除桶上的清单规则，避免后续继续产出清单文件造成残留。
