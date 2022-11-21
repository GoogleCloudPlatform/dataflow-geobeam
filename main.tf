terraform {
  backend "gcs" {
    bucket = "geobeam-tfstate"
  }
}
provider "google" {
  project     = "dataflow-geobeam"
  region      = "us-central1"
}

module "bigquery" {
  source  = "terraform-google-modules/bigquery/google"
  version = "4.3.0"
  dataset_id = "examples"
  dataset_name = "examples"
  description = "geobeam test dataset"
  project_id = "dataflow-geobeam"
  location = "US"
  default_table_expiration_ms = null
  delete_contents_on_destroy = false
  tables = [
    {
      table_id = "parcel"
      schema = "geobeam/examples/parcel_schema.json"
      time_partitioning = null
      clustering = ["geom"]
      expiration_time = null
      labels = null
    },
    {
      table_id = "dem"
      schema = "geobeam/examples/dem_schema.json"
      time_partitioning = null
      clustering = ["geom"]
      expiration_time = null
      labels = null
    },
    {
      table_id = "soilgrid"
      schema = "geobeam/examples/soilgrid_schema.json"
      time_partitioning = null
      clustering = ["geom"]
      expiration_time = null
      labels = null
    },
    {
      table_id = "FLD_HAZ_AR"
      schema = "geobeam/examples/FLD_HAZ_AR_schema.json"
      time_partitioning = null
      clustering = ["FLD_ZONE", "ZONE_SUBTY"]
      expiration_time = null
      labels = null
    },
    {
      table_id = "CSLF_Ar"
      schema = "geobeam/examples/S_CSLF_Ar_schema.json"
      time_partitioning = null
      clustering = ["PRE_ZONE", "PRE_ZONEST", "NEW_ZONE", "NEW_ZONEST"]
      expiration_time = null
      labels = null
    }
  ]
}

data "google_iam_policy" "public_bucket" {
  binding {
    role = "roles/storage.objectViewer"
    members = ["allUsers"]
  }
}
resource "google_storage_bucket" "geobeam_public_bucket" {
  name          = "geobeam"
  location      = "US"
  force_destroy = false
}
resource "google_storage_bucket_iam_policy" "public_rule" {
  bucket = google_storage_bucket.geobeam_public_bucket.name
  policy_data = data.google_iam_policy.public_bucket.policy_data
}
resource "google_storage_bucket" "pipeline_tmp_bucket" {
  name          = "geobeam-pipeline-tmp"
  location      = "US"
  force_destroy = false
}
