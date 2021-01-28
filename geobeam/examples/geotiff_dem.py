# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Example pipeline that loads a DEM (digital elevation model) raster into
Bigquery using the `STREAMING_INSERTS` method.

`STREAMING_INSERTS` is suitable for small-ish geometries, such as points or
simple polygons.
"""

import logging
import json
import argparse

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.options.pipeline_options import PipelineOptions


def format_record(element, band_column):
    """
    Format the tuple received from GeotiffSource into a record readable by
    `beam.io.WriteToBigQuery()`. Convert the floating-point meters into
    rounded centimeters to store as INT64 in order to support clustering
    on this value column (elev).
    """

    value, geom = element

    return {
        band_column: int(value * 100),
        'geom': json.dumps(geom)
    }


def run(pipeline_args, known_args):
    """
    Run the pipeline
    """

    from geobeam.io import GeotiffSource

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(GeotiffSource(known_args.gcs_url,
             band_number=known_args.band_number,
             skip_nodata=known_args.skip_nodata,
             centroid_only=known_args.centroid_only,
             in_epsg=known_args.in_epsg))
         | 'FormatRecords' >> beam.Map(format_record, known_args.band_column)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             method='STREAMING_INSERTS',
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs_url')
    parser.add_argument('--dataset')
    parser.add_argument('--table')
    parser.add_argument('--band_column')
    parser.add_argument('--band_number', type=int, default=1)
    parser.add_argument('--skip_nodata', type=bool, default=True)
    parser.add_argument('--centroid_only', type=bool, default=False)
    parser.add_argument('--in_epsg', type=int, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
