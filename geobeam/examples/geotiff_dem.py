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
Bigquery using the BlockSource method.
"""


def elev_to_centimeters(element):
    """
    Convert the floating-point meters into rounded centimeters to store
    as INT64 in order to support clustering on this value column (elev).
    """

    band_data, geom = element

    for i in range(0, len(band_data)):
        for j in range(0, len(band_data[i])):
            band_data[i][j] = int(band_data[i][j] * 100)

    return band_data, geom


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
         | beam.io.Read(RasterBlockSource(known_args.gcs_url, bidx=1))
         | 'ElevToCentimeters' >> beam.Map(elev_to_centimeters)
         | 'FormatRecords' >> beam.Map(format_rasterblock_record, {1: 'elev'})
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             schema=known_args.schema,
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
    parser.add_argument('--schema')
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
