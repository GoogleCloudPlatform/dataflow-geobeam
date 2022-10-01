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
Example pipeline that loads a county parcel shape dataset into BigQuery.
"""

def orient_polygon(element):
    from shapely.geometry import shape, polygon, MultiPolygon

    props, geom = element
    geom_shape = shape(geom)

    if geom_shape.geom_type == 'Polygon':
        oriented_geom = polygon.orient(geom_shape)
        return props, oriented_geom

    if geom_shape.geom_type == 'MultiPolygon':
        pgons = []
        for pgon in geom_shape.geoms:
            pgons.append(polygon.orient(pgon))
            oriented_mpgon = MultiPolygon(pgons)
        return props, oriented_mpgon

    return props, geom


def typecast_fields(record):
    return {
        **record,
        'LRSN': str(record['LRSN'])
    }


def run(pipeline_args, known_args):
    """
    Invoked by the Beam runner
    """

    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
    from geobeam.io import ShapefileSource
    from geobeam.fn import format_record, make_valid, filter_invalid

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(ShapefileSource(known_args.gcs_url,
             layer_name=known_args.layer_name))
         | 'OrientPolygon' >> beam.Map(orient_polygon)
         #| 'MakeValid' >> beam.Map(make_valid)
         #| 'FilterInvalid' >> beam.Filter(filter_invalid)
         | 'FormatRecords' >> beam.Map(format_record)
         | 'TypecastRecords' >> beam.Map(typecast_fields)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
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
    parser.add_argument('--in_epsg', type=int, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
