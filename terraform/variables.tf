variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "credentials" {
  description = "Path to your service account key file"
  type        = string
}

variable "bq_ods_members" {
  description = "Members of the ODS gmaps tables"
  type        = list(string)
}

variable "bq_ods_places_tablename" {
  description = "Table name of the ODS places bigquery table"
  type        = string
  default     = "ods-gmaps-places"
}

variable "bq_ods_reviews_tablename" {
  description = "Table name of the ODS reviews bigquery table"
  type        = string
  default     = "ods-gmaps-reviews"
}
