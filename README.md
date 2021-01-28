# dataflow-geobeam

geobeam adds GIS capabilities to your Apache Beam pipelines.

## What does geobeam do?

`geobeam` enables you to ingest and analyze massive amounts of geospatial data in parallel using [Dataflow](https://cloud.google.com/dataflow).
geobeam installs GDAL, PROJ4, and other related libraries onto your
Dataflow worker machines, and provides a set of [FileBasedSource](https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.io.filebasedsource.html)
classes that make it easy to read, process, and write geospatial data. `geobeam` can also
understand vector layer definitions and auto-generate Bigquery schemas.

### Supported input types

| **File format** | **Data type** | **Geobeam class**  |
|:----------------|:--------------|:-------------------|
| `tiff`         | raster        | `GeotiffSource`
| `shp`          | vector        | `ShapefileSource`
| `gdb`          | vector        | `GeodatabaseSource`

### Included libraries

`geobeam` includes several python modules that allow you to perform a wide variety of operations and analyses on your geospatial data.

| **Module**      | **Version** | **Description** |
|:----------------|:------------|:----------------|
| [gdal](https://pypi.org/project/GDAL/)          | 3.2.1       | python bindings for GDAL
| [rasterio](https://pypi.org/project/rasterio/)  | 1.1.8       | reads and writes geospatial raster data
| [fiona](https://pypi.org/project/Fiona/)        | 1.8.18      | reads and writes geospatial vector data
| [shapely](https://pypi.org/project/Shapely/)    | 1.7.1       | manipulation and analysis of geometric objects in the cartesian plane
| [pyproj](https://pypi.org/project/pyproj/)      | 3.0.0       | cartographic projections and coordinate transformations library

### Dataflow templates

| **Template**              | **Description** |
|:--------------------------|:----------------|
| GeoTiff -> Bigquery       | polygonize a Geotiff raster and load into Bigquery
| Shapefile -> Bigquery     | load a shapefile layer into Bigquery
| Geodatabase -> Bigquery   | load a geodatabase layer into Bigquery


## How to Use

Use the `geobeam` python module to build a custom pipeline.

1. Install the module
```
pip install geobeam
```

2. Write a Dockerfile to build a [custom container](https://cloud.google.com/dataflow/docs/guides/using-custom-containers) based on the [`dataflow-geobeam/base`](Dockerfile) image.
*Note:* make sure you use a version-tagged image, e.g. `3.2.1`. Do not use `latest`.

```dockerfile
FROM gcr.io/dataflow-geobeam/base:3.2.1
COPY . .
```

```bash
# build locally with docker
docker build -t gcr.io/<project_id>/example
docker push gcr.io/<project_id>/example
```

3. Run in Dataflow

```
python -m geobeam.examples.geotiff_dem
  --runner DataflowRunner
  --worker_harness_container_image=gcr.io/<project_id>/example
  --experiment use_runner_v2
  --temp_location gs://<bucket>
  --gcs_url <input_file>
  --dataset=geobeam
  --table=dem
  --band_column=elev
  --centroid_only true
```


#### Examples

##### Polygonize Raster
```py
def format_record(element, band_column):
  (value, geom) = element
  return { band_column: value, 'geom': json.dumps(geom) }

def run(options):
  from geobeam.io import GeotiffSource

  with beam.Pipeline(options) as p:
    (p  | 'ReadRaster' >> beam.io.Read(GeotiffSource(gcs_url))
        | 'FormatRecord' >> beam.Map(format_record, 'elev')
        | 'WriteToBigquery' >> beam.io.WriteToBigQuery('geo.dem'))
```

##### Validate and Simplify Shapefile

```py
def validate_and_simplify(element):
  from osgeo import ogr
  import json

  (props, geom) = element

  ogr_geom = ogr.CreateGeometryFromJson(json.dumps(geom))
  ogr_geom = ogr_geom.MakeValid()
  ogr_geom.SimplifyPreserveTopology(0.00001)

  return { **props, 'geom': ogr_geom.ExportToJson() }

def run(options):
  from geobeam.io import ShapefileSource

  with beam.Pipeline(options) as p:
    (p  | 'ReadShapefile' >> beam.io.Read(ShapefileSource(gcs_url))
        | 'ValidateAndSimplify' >> beam.Map(validate_and_simplify)
        | 'WriteToBigquery' >> beam.io.WriteToBigQuery('geo.parcel'))
```

See `geobeam/examples/` for complete examples.

## Examples

A number of example pipelines are available in the `geobeam/examples/` folder.
To run them in your Google Cloud project, run the included [terraform](https://www.terraform.io/) file to set up the Bigquery dataset and tables.

Open up Bigquery GeoViz to visualize your data.

![](https://storage.googleapis.com/geobeam/examples/geobeam-nfhl-geoviz-example.png)


## License

This is not an officially supported Google product, though support will be provided on a best-effort basis.

```
Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
