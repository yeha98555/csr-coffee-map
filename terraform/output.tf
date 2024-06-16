output "etl_raw_bucket_url" {
  value = "gs://${google_storage_bucket.etl_raw.name}"
}

output "etl_processed_bucket_url" {
  value = "gs://${google_storage_bucket.etl_processed.name}"
}

output "etl_archive_bucket_url" {
  value = "gs://${google_storage_bucket.etl_archive.name}"
}
