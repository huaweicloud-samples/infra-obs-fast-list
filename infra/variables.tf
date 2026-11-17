variable "region" {
  description = "资源部署区域"
  type        = string
  default     = "cn-north-4"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.region))
    error_message = "区域格式形如 cn-north-4。"
  }
}

variable "name_prefix" {
  description = "资源名称前缀，用于生成桶名"
  type        = string
  default     = "obsfast"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,28}[a-z0-9]$", var.name_prefix))
    error_message = "前缀只能包含小写字母、数字与连字符，长度 3-30。"
  }
}

variable "source_bucket_name" {
  description = "待盘点的源桶名称；留空时按名称前缀自动生成（桶名全局唯一）"
  type        = string
  default     = ""
}

variable "inventory_bucket_name" {
  description = "存放桶清单文件的目标桶名称；留空时按名称前缀自动生成（桶名全局唯一）"
  type        = string
  default     = ""
}

variable "enable_versioning" {
  description = "是否为源桶开启多版本"
  type        = bool
  default     = false
}

variable "enable_encryption" {
  description = <<-EOT
    是否为源桶开启默认服务端加密。

    ⚠️ 默认关闭，原因是该开关对应 KMS 默认加密，而 KMS 加密会同时影响两项能力：
      1. 列举结果中的 ETag 不再是对象内容的摘要，且同内容重复上传取值会变化，
         导致「按内容标识比对」失效（工具会自动降级为仅按大小判定）；
      2. 桶不能作为桶清单的投递目标（服务端会拒绝配置）。
    如需开启，请确认业务不依赖上述能力，或改用工具的大小判定口径。
  EOT
  type        = bool
  default     = false
}

variable "inventory_lifecycle_days" {
  description = "清单文件在目标桶中的保留天数，到期自动删除"
  type        = number
  default     = 30

  validation {
    condition     = var.inventory_lifecycle_days >= 1 && var.inventory_lifecycle_days <= 3650
    error_message = "保留天数需在 1 至 3650 之间。"
  }
}

variable "abort_multipart_days" {
  description = "未完成的分段上传在多少天后自动清理"
  type        = number
  default     = 7
}

variable "bucket_quota" {
  description = "源桶容量上限（字节）；0 表示不限制"
  type        = number
  default     = 0
}

variable "tags" {
  description = "附加到所有资源的标签"
  type        = map(string)
  default = {
    project = "obsfast"
  }
}
