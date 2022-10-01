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
Example pipeline that prints a geotiff (for testing)
"""

def format_raster_record(element):
    import json, numpy

    data, geom = element

    #print(numpy.asarray(data[0]))

    return {
        'band_1': numpy.asarray(data[0]).tolist(),
        'geom': json.dumps(geom)
    }


def run(pipeline_args, known_args):
    """
    Run the pipeline. Invoked by the Beam runner.
    """
    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions

    from geobeam.io import RasterBlockSource
    from geobeam.fn import format_rasterblock_record

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink'
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(RasterBlockSource(known_args.gcs_url))
         | 'FormatRasterRecord' >> beam.Map(format_rasterblock_record)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 projectId='dataflow-geobeam',
                 datasetId='examples',
                 tableId='raster_block_test'),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

         #| 'FormatRecords' >> beam.Map(format_record, known_args.band_column, 'int')

if __name__ == '__main__':
    import logging
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs_url')
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
