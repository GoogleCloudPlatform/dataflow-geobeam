provider "google" {
  project     = "geobeam-301922"
  region      = "us-central1"
}

module "bigquery" {
  source  = "terraform-google-modules/bigquery/google"
  version = "4.3.0"
  dataset_id = "geobeam"
  dataset_name = "geobeam"
  description = "geobeam test"
  project_id = "geobeam-301922"
  location = "US"
  default_table_expiration_ms = null
  delete_contents_on_destroy = false
  tables = [
    {
      table_id = "parcel"
      schema = "./parcel_schema.json"
      time_partitioning = null
      clustering = ["geom_bbox"]
      expiration_time = null
      labels = null
    },
    {
      table_id = "dem"
      schema = "./dem_schema.json"
      time_partitioning = null
      clustering = ["geom", "elev"]
      expiration_time = null
      labels = null
    }
  ]
}

resource "google_storage_bucket" "geobeam_public_bucket" {
  name          = "geobeam"
  location      = "US"
  force_destroy = false
}
resource "google_storage_bucket_access_control" "public_rule" {
  bucket = google_storage_bucket.geobeam_public_bucket.name
  role   = "READER"
  entity = "allUsers"
}
resource "google_storage_bucket" "pipeline_tmp_bucket" {
  name          = "geobeam-pipeline-tmp"
  location      = "US"
  force_destroy = false
}
