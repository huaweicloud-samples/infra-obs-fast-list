# 对象存储盘点工具的运行底座：
#   1) 源桶——被盘点的目标；
#   2) 清单桶——承接服务端生成的桶清单文件。
#
# 说明：桶清单规则本身（生成频率、目标路径、可选字段）由工具的命令行
# 接口下发，详细说明见 docs/deployment-guide.md。
# 当前 Terraform Provider 未提供该资源的托管能力，因此不在本目录声明。

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  source_bucket_name = (
    var.source_bucket_name != ""
    ? var.source_bucket_name
    : "${var.name_prefix}-source-${random_id.suffix.hex}"
  )

  inventory_bucket_name = (
    var.inventory_bucket_name != ""
    ? var.inventory_bucket_name
    : "${var.name_prefix}-inventory-${random_id.suffix.hex}"
  )

  common_tags = merge(
    {
      project     = "obsfast"
      managed_by  = "terraform"
      environment = "practice"
    },
    var.tags,
  )
}

# ---------------------------------------------------------------------------
# 源桶：被并发盘点的对象存储桶
# ---------------------------------------------------------------------------

resource "huaweicloud_obs_bucket" "source" {
  bucket        = local.source_bucket_name
  acl           = "private"
  storage_class = "STANDARD"
  versioning    = var.enable_versioning
  encryption    = var.enable_encryption
  quota         = var.bucket_quota > 0 ? var.bucket_quota : null
  tags          = local.common_tags

  lifecycle_rule {
    name    = "cleanup-incomplete-multipart"
    enabled = true
    prefix  = ""

    abort_incomplete_multipart_upload {
      days = var.abort_multipart_days
    }
  }
}

# ---------------------------------------------------------------------------
# 清单桶：存放服务端生成的桶清单文件
#
# ⚠️ 该桶刻意不启用服务端加密：服务端拒绝把桶清单投递到 KMS 默认加密的桶
#    （配置时会返回 InvalidArgument: The destination bucket is KMS encryption bucket）。
#    如确有加密要求，请改用非 KMS 的加密方式，或在目标桶之外单独承接清单。
# ---------------------------------------------------------------------------

resource "huaweicloud_obs_bucket" "inventory" {
  bucket        = local.inventory_bucket_name
  acl           = "private"
  storage_class = "STANDARD"
  tags          = merge(local.common_tags, { role = "inventory-destination" })

  # 清单文件按周期滚动生成，历史报告按保留天数自动清理，避免长期堆积
  lifecycle_rule {
    name    = "expire-inventory-reports"
    enabled = true
    prefix  = ""

    expiration {
      days = var.inventory_lifecycle_days
    }
  }
}
