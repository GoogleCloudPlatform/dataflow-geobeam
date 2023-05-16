# Copyright 2023 Google LLC
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
Reproject an existing table in BQ that has WKT in a string column
"""


def serialize_geometries(row, in_crs):
    import pyproj
    from shapely import wkt
    from shapely.ops import transform

    src = pyproj.CRS(in_crs)
    wgs84 = pyproj.CRS('EPSG:4326')

    geom_wkt = row['geometry']
    geom = wkt.loads(geom_wkt)

    project = pyproj.Transformer.from_crs(src, wgs84, always_xy=True).transform
    geom = transform(project, geom)

    geom_wkt = wkt.dumps(geom)

    return {
        **row,
        'geometry': geom_wkt
    }


def get_table_schema(table_spec):
    import io
    from google.cloud import bigquery
    from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
    import json

    client = bigquery.Client()
    table = client.get_table(table_spec.replace(':', '.'))
    f = io.StringIO("")

    client.schema_to_json(table.schema, f)

    schema = f.getvalue()
    schema = json.dumps({ 'fields': json.loads(f.getvalue()) })

    return parse_table_schema_from_json(schema)


def run(pipeline_args, known_args):
    """
    Run the pipeline
    """

    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions

    from geobeam.fn import make_valid, filter_invalid, format_record

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
    ] + pipeline_args)

    src_crs = 'epsg:{}'.format(known_args.src_epsg)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'ReadTable' >> beam.io.ReadFromBigQuery(table=known_args.in_table)
         | 'SerializeGeometries' >> beam.Map(serialize_geometries, src_crs)
         #| 'MakeValid' >> beam.Map(make_valid)
         #| 'FilterInvalid' >> beam.Filter(filter_invalid)
         #| 'FormatRecords' >> beam.Map(format_record)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             known_args.out_table,
             schema=get_table_schema(known_args.in_table),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    import logging
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--in_table')
    parser.add_argument('--out_table')
    parser.add_argument('--src_epsg', type=int, default=None, required=True)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
