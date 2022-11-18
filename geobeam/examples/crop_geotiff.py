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
Example pipeline that loads a cropland type raster dataset into
Bigquery.
"""


def run(pipeline_args, known_args):
    """
    Run the pipeline. Invoked by the Beam runner.
    """
    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions

    from geobeam.io import RasterPolygonSource
    from geobeam.fn import format_rasterpolygon_record

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink'
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:

        logging.info('Will be inserting column: {}'.format(known_args.band_column))

        (p
         | beam.io.Read(RasterPolygonSource(known_args.gcs_url, bidx=known_args.band_number))
         | 'FormatRecords' >> beam.Map(format_rasterpolygon_record, band_type='int',
            band_column=known_args.band_column)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == '__main__':
    import logging
    import argparse

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
    parser.add_argument('--merge_blocks', type=int, default=1)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
