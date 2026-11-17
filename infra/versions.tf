terraform {
  required_version = ">= 1.5.0"

  required_providers {
    huaweicloud = {
      source  = "huaweicloud/huaweicloud"
      version = "~> 1.97"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

# 凭据通过环境变量提供，不写入任何配置文件：
#   export HW_ACCESS_KEY="<访问密钥 ID>"
#   export HW_SECRET_KEY="<访问密钥>"
# 区域由 var.region 指定，也可通过 HW_REGION 环境变量提供。
provider "huaweicloud" {
  region = var.region
}
