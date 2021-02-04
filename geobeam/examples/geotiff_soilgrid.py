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
Example pipeline that loads a Soil Grid raster that contains groundwater
saturation values.
"""


def run(pipeline_args, known_args):
    """
    Run the pipeline. Invoked by the Beam runner.
    """

    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions

    import geobeam.fn
    from geobeam.io import GeotiffSource

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink'
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(GeotiffSource(known_args.gcs_url,
             band_number=known_args.band_number,
             merge_blocks=known_args.merge_blocks))
         | 'MakeValid' >> beam.Map(geobeam.fn.make_valid)
         | 'FilterInvalid' >> beam.Filter(geobeam.fn.filter_invalid)
         | 'FormatRecords' >> beam.Map(geobeam.fn.format_record,
             known_args.band_column, known_args.band_type)
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
    parser.add_argument('--band_column', default='value')
    parser.add_argument('--band_number', type=int, default=1)
    parser.add_argument('--band_type', type=str, default='int')
    parser.add_argument('--merge_blocks', type=int, default=1)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
