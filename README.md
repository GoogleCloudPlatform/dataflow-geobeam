geobeam adds GIS capabilities to your Apache Beam pipelines.

## What does geobeam do?

`geobeam` enables you to ingest and analyze massive amounts of geospatial data in parallel using [Dataflow](https://cloud.google.com/dataflow).
geobeam installs GDAL, PROJ4, and other related libraries onto your
Dataflow worker machines, and provides a set of [FileBasedSource](https://beam.apache.org/releases/pydoc/2.25.0/apache_beam.io.filebasedsource.html)
classes that make it easy to read, process, and write geospatial data. `geobeam` also provides a set of helpful
Apache Beam transforms to use in your pipelines. 

See the [Full Documentation](https://storage.googleapis.com/geobeam/docs/all.pdf) for complete API specification.

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


## How to Use

Use the `geobeam` python module to build a custom pipeline.

1. Install the module
```
pip install geobeam
```

2. Write a Dockerfile to build a [custom container](https://cloud.google.com/dataflow/docs/guides/using-custom-containers) based on the [`dataflow-geobeam/base`](Dockerfile) image.

```dockerfile
FROM gcr.io/dataflow-geobeam/base
COPY . .
```

```bash
# build locally with docker
docker build -t gcr.io/<project_id>/example
docker push gcr.io/<project_id>/example
```

3. Run in Dataflow

```
# run the geotiff_soilgrid example in dataflow
python -m geobeam.examples.geotiff_soilgrid
  --gcs_url gs://geobeam/examples/AWCh3_M_sl1_250m_ll.tif
  --dataset=examples
  --table=soilgrid
  --band_column=h3
  --runner=DataflowRunner
  --worker_harness_container_image=gcr.io/dataflow-geobeam/example
  --experiment=use_runner_v2
  --temp_location=<temp bucket>
  --service_account_email <service account>
  --region us-central1
  --max_num_workers 6
  --machine_type c2-standard-16
  --merge_blocks 64
```


#### Examples

##### Polygonize Raster
```py
def run(options):
  from geobeam.io import GeotiffSource
  from geobeam.fn import format_record

  with beam.Pipeline(options) as p:
    (p  | 'ReadRaster' >> beam.io.Read(GeotiffSource(gcs_url))
        | 'FormatRecord' >> beam.Map(format_record, 'elev', 'float')
        | 'WriteToBigquery' >> beam.io.WriteToBigQuery('geo.dem'))
```

##### Validate and Simplify Shapefile

```py
def run(options):
  from geobeam.io import ShapefileSource
  from geobeam.fn import make_valid, filter_invalid, format_record

  with beam.Pipeline(options) as p:
    (p  | 'ReadShapefile' >> beam.io.Read(ShapefileSource(gcs_url))
        | 'Validate' >> beam.Map(make_valid)
        | 'FilterInvalid' >> beam.Filter(filter_invalid)
        | 'FormatRecord' >> beam.Map(format_record)
        | 'WriteToBigquery' >> beam.io.WriteToBigQuery('geo.parcel'))
```

See `geobeam/examples/` for complete examples.

## Examples

A number of example pipelines are available in the `geobeam/examples/` folder.
To run them in your Google Cloud project, run the included [terraform](https://www.terraform.io) file to set up the Bigquery dataset and tables used by the example pipelines.

Open up Bigquery GeoViz to visualize your data.

![](https://storage.googleapis.com/geobeam/examples/geobeam-nfhl-geoviz-example.png)

## Included Transforms

The `geobeam.fn` module includes several [Beam Transforms](https://beam.apache.org/documentation/programming-guide/#transforms) that you can use in your pipelines.

| **Module**      | **Description**
|:----------------|:------------|
| `geobeam.fn.make_valid`     | Attempt to make all geometries valid. 
| `geobeam.fn.filter_invalid` | Filter out invalid geometries that cannot be made valid
| `geobeam.fn.format_record`  | Format the (props, geom) tuple received from a FileSource into a `dict` that can be inserted into the destination table


## Execution parameters

Each FileSource accepts several parameters that you can use to configure how your data is loaded and processed.
These can be parsed as pipeline arguments and passed into the respective FileSources as seen in the examples pipelines.

| **Parameter**      | **Input type** | **Description** | **Default** | **Required?**
|:-------------------|:---------------|:----------------|:------------|---------------|
| `skip_reproject`   | All     | True to skip reprojection during read | `False` | No
| `in_epsg`          | All     | An [EPSG integer](https://en.wikipedia.org/wiki/EPSG_Geodetic_Parameter_Dataset) to override the input source CRS to reproject from | | No
| `band_number`      | Raster  | The raster band to read from | `1` | No
| `include_nodata`   | Raster  | True to include `nodata` values | `False` | No
| `centroid_only`    | Raster  | True to only read pixel centroids | `False` | No
| `merge_blocks`     | Raster  | Number of block windows to combine during read. Larger values will generate larger, better-connected polygons. | | No
| `layer_name`       | Vector  | Name of layer to read | | Yes
| `gdb_name`         | Vector  | Name of geodatabase directory in a gdb zip archive | | Yes, for GDB files


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
