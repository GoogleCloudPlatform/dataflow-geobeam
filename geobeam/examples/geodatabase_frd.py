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
Loads FRD (Flood Risk Database) layers into Bigquery using the `FILE_LOADS`
insertion method. `FILE_LOADS` should be used when individual geometries
can be large and complex to avoid the size limits of the `STREAMING_INSERTS`
method. See: https://cloud.google.com/bigquery/quotas#streaming_inserts.
"""


def run(pipeline_args, known_args):
    """
    Run the pipeline
    """

    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

    from geobeam.io import GeodatabaseSource
    from geobeam.fn import make_valid, filter_invalid, format_record

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
        '--setup_file',  '/dataflow/template/setup.py'
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(GeodatabaseSource(known_args.gcs_url,
             layer_name=known_args.layer_name,
             gdb_name=known_args.gdb_name))
         | 'MakeValid' >> beam.Map(make_valid)
         | 'FilterInvalid' >> beam.Filter(filter_invalid)
         | 'FormatRecords' >> beam.Map(format_record)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             custom_gcs_temp_location=known_args.custom_gcs_temp_location,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))


if __name__ == '__main__':
    import logging
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs_url')
    parser.add_argument('--dataset')
    parser.add_argument('--table')
    parser.add_argument('--layer_name')
    parser.add_argument('--gdb_name')
    parser.add_argument('--in_epsg', type=int, default=None)
    parser.add_argument('--custom_gcs_temp_location')
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
