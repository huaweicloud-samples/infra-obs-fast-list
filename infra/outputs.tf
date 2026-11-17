output "source_bucket_name" {
  description = "源桶名称，用于工具命令行的 --bucket 参数"
  value       = huaweicloud_obs_bucket.source.bucket
}

output "inventory_bucket_name" {
  description = "清单桶名称，用于工具命令行的 --destination-bucket 参数"
  value       = huaweicloud_obs_bucket.inventory.bucket
}

output "region" {
  description = "资源所在区域"
  value       = var.region
}

output "source_bucket_endpoint" {
  description = "源桶的服务端点"
  value       = huaweicloud_obs_bucket.source.bucket_domain_name
}

output "suggested_inventory_manifest_prefix" {
  description = "清单报告的推荐前缀，报告位置的完整形态为 {前缀}/{源桶名}/{清单名称}/{时间戳}/manifest.json"
  value       = "inventory-reports"
}
