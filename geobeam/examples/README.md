# geobeam Examples

## `shapefile_parcel`

Load a shapefile of county parcels into Bigquery

### Run locally

```
python -m geobeam.examples.shapefile_parcel \
  --runner DirectRunner \
  --project <your project>
  --temp_location gs://geobeam-pipeline-tmp \
  --gcs_url gs://geobeam/examples/ghent-parcels-shp.zip \
  --layer_name Property_Information \
  --dataset examples \
  --table parcel
```

### Run in Dataflow

```
python -m geobeam.examples.shapefile_parcel \
  --runner DataflowRunner \
  --worker_harness_container_image gcr.io/dataflow-geobeam/example \
  --experiment use_runner_v2 \
  --project=dataflow-geobeam \
  --temp_location <your temp bucket>
  --service_account_email <your service account>
  --region us-central1
  --gcs_url gs://geobeam/examples/ghent-parcels-shp.zip \
  --layer_name Property_Information \
  --dataset examples \
  --table parcel
```

## `geodatabase_frd`

### Run locally

```
python -m geobeam.examples.geodatabase_frd \
  --runner DirectRunner \
  --project <your project> \
  --temp_location <your temp bucket> \
  --gcs_url gs://geobeam/examples/FRD_510104_Coastal_GeoDatabase_20160708.zip \
  --dataset examples \
  --table CSLF_Ar \
  --gdb_name FRD_510104_Coastal_GeoDatabase_20160708.gdb \
  --layer_name S_CSLF_Ar
```

### Run in Dataflow

```
python -m geobeam.examples.geodatabase_frd \
  --project <your project> \
  --runner DataflowRunner \
  --worker_harness_container_image gcr.io/dataflow-geobeam/example \
  --experiment use_runner_v2 \
  --temp_location <your temp bucket> \
  --service_account_email <your service account> \
  --region us-central1 \
  --gcs_url gs://geobeam/examples/FRD_510104_Coastal_GeoDatabase_20160708.zip \
  --gdb_name FRD_510104_Coastal_GeoDatabase_20160708.gdb \
  --layer_name S_CSLF_Ar \
  --dataset examples \
  --table CSLF_Ar
```

## `geotiff_dem`

Load a Digital Elevation Model (DEM) raster into Bigquery

### Run Locally

```
python -m geobeam.examples.geotiff_dem \
  --runner DirectRunner \
  --temp_location <your temp bucket> \
  --project <your project> \
  --gcs_url gs://geobeam/examples/ghent-dem-1m.tif \
  --band_column elev \
  --centroid_only true \
  --skip_nodata true
```

### Run in Dataflow

```
python -m geobeam.examples.geotiff_dem \
  --runner DataflowRunner \
  --worker_harness_container_image gcr.io/dataflow-geobeam/example \
  --experiment use_runner_v2 \
  --project dataflow-geobeam \
  --temp_location gs://geobeam-pipeline-tmp/ \
  --service_account_email dataflow-runner@dataflow-geobeam.iam.gserviceaccount.com \
  --region us-central1 \
  --gcs_url gs://geobeam/examples/dem-clipped-test.tif \
  --dataset examples \
  --table dem \
  --schema 'elev:INT64,geom:GEOGRAPHY'
  --band_column elev \
  --max_num_workers 3 \
  --machine_type c2-standard-30 \
  --merge_blocks 80 \
  --centroid_only true \
```


## `geotiff_soilgrid`

### Run Locally

```
python -m geobeam.examples.geotiff_soilgrid \
  --runner DirectRunner \
  --project <your project> \
  --temp_location <your temp bucket> \
  --gcs_url gs://geobeam/examples/soilgrid-test-clipped.tif \
  --dataset examples \
  --table soilgrid \
  --band_column h3
```


### Run in Dataflow

```
python -m geobeam.examples.geotiff_soilgrid \
  --runner DataflowRunner \
  --worker_harness_container_image gcr.io/dataflow-geobeam/example \
  --experiment use_runner_v2 \
  --temp_location <your temp bucket> \
  --project <your project> \
  --service_account_email <your service account> \
  --region us-central1 \
  --machine_type c2-standard-8 \
  --gcs_url gs://geobeam/examples/soilgrid-test-clipped.tif \
  --merge_blocks 20 \
  --dataset examples \
  --table soilgrid \
  --band_column h3
```

## `shapefile_nfhl`

### Run Locally

```
python -m geobeam.examples.shapefile_nfhl \
  --runner DirectRunner \
  --project <your project> \
  --temp_location <your temp bucket> \
  --gcs_url gs://geobeam/examples/510104_20170217.zip \
  --dataset examples \
  --table FLD_HAZ_AR \
  --layer_name S_FLD_HAZ_AR
```

### Run in Dataflow

```
python -m geobeam.examples.shapefile_nfhl \
  --runner DataflowRunner \
  --project <your project> \
  --temp_location <your temp bucket> \
  --worker_harness_container_image gcr.io/dataflow-geobeam/example \
  --experiment use_runner_v2 \
  --service_account_email <your service account> \
  --gcs_url gs://geobeam/examples/510104_20170217.zip \
  --layer_name S_FLD_HAZ_AR \
  --dataset examples \
  --table FLD_HAZ_AR
```

## `streaming_pubsub`

### Run Locally

#### setup pubsub emulator
```
gcloud components install pubsub-emulator
gcloud beta emulators pubsub start &
curl -X PUT -v http://localhost:8085/v1/projects/example-project/topics/example-topic
```

#### run pipeline
```
PUBSUB_EMULATOR_HOST=localhost:8085 python -m geobeam.examples.streaming_pubsub \
  --runner DirectRunner \
  --streaming \
  --in_proj "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"
```

#### publish messages to topic

```
curl -v \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@geobeam/examples/pubsub_emulator_messages.json" \
  http://localhost:8085/v1/projects/example-project/topics/example-topic:publish
```

#### unfortunate note
There is a bug in the pubsub emulator, or the directrunner, that garbles the
message timestamp from the emulator. Until this is figured out, do the following:

// TODO

