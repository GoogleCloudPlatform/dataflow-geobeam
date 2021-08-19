# Getting started with geobeam!

This tutorial will show you how to load Flood Zone data into BigQuery using geobeam.

Before you begin, make sure you've created a project that you can run geobeam in, and set the current project as follows:

```bash
gcloud config set project [PROJECT_ID]
```

## 1. Setup cloud infrastructure

**geobeam** needs some infrastructure set up before you can begin loading data.

Run the following commands in your shell to set up BigQuery (see [Required Permissions](https://cloud.google.com/bigquery/docs/datasets#required_permissions):
```bash
gcloud services enable bigquery
bq mk -d geobeam
bq mk -t geobeam.flood_zone
```

And set up a bucket for BigQuery temp storage (generates a [globally-unique name](https://cloud.google.com/storage/docs/naming-buckets#considerations)):
```bash
gsutil mb gs://pipeline-temp-$(tr -dc A-Za-z0-9 </dev/urandom | head -c 8 ; echo '')
```

###
