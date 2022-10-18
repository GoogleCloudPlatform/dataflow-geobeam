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

import apache_beam as beam
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import geobeam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from geobeam.io import ShapefileSource
from geobeam.fn import format_record, make_valid, filter_invalid

from google.cloud import storage
import fiona
import json
from fiona import BytesCollection


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


def create_table(known_args,pipeline_args):

    gcs_url = known_args.gcs_url
    bucket_name = gcs_url.split('/')[2]
    file_name = '/'.join(gcs_url.split('/')[3:])
    zip_name = gcs_url.split('/')[-1].split('.')[0]
  
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket_name).get_blob(file_name)
    source_bucket = storage_client.bucket(bucket_name)
    blob_uri = gcs_url

    blob_2 = source_bucket.blob(file_name)
    data = blob.download_as_string()
    
    profile = None
    layer_name= known_args.layer_name

    if layer_name is not None:
        with fiona.io.ZipMemoryFile(data) as zip:
            with zip.open(f'{layer_name}.shp',layer=layer_name) as collection:
                print(collection)
                profile = collection.profile
    elif layer_name is not None:
        profile = BytesCollection(data, layer=layer_name).profile
    else:
        profile = fiona.open(gcs_url).profile
            
    from fiona import prop_type

    BQ_FIELD_TYPES = {
        'int': 'INTEGER',
        'str': 'STRING',
        'float': 'FLOAT',
        'bool': 'BOOL',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'DATETIME',
        'bytes': 'BYTES'
    }

    bq_schema = []

    for field_name, field_type in profile['schema']['properties'].items():
        fiona_type = prop_type(field_type)
        bq_type = BQ_FIELD_TYPES[fiona.schema.FIELD_TYPES_MAP_REV[fiona_type]]
        bq_schema.append({
            'name': field_name,
            'type': bq_type
        })

    bq_schema.append({
        'name': 'geom',
        'type': 'GEOGRAPHY',
           })
    
    schema_json = json.JSONEncoder(sort_keys=True).encode(bq_schema)

    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    #table_id = format(known_args.project, known_args.dataset, known_args.table)
    #project_id = "vadimzaripov-477-2022062208552"
    
    #TODO: FIX THE DAMNED PROJECT ID 
    
    beam_options = PipelineOptions(pipeline_args)
    options = list(beam_options.display_data().values())
    table_id=f"{options}.{known_args.dataset}.{known_args.table}"
    
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        table = client.delete_table(table_id)
    except NotFound:
         print("Table {} is not found.".format(table_id))
         
    
    bigquerySchema = []

    bigqueryColumns = json.loads(schema_json)
    for col in bigqueryColumns:
        bigquerySchema.append(bigquery.SchemaField(col['name'], col['type']))

    table = bigquery.Table(table_id, schema=bigquerySchema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def run(pipeline_args, known_args):
    """
    Invoked by the Beam runner
    """
    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.Read(ShapefileSource(known_args.gcs_url,
             layer_name=known_args.layer_name))
         | 'MakeValid' >> beam.Map(make_valid)
         | 'FilterInvalid' >> beam.Filter(filter_invalid)
         | 'OrientPolygon' >> beam.Map(orient_polygon)
         | 'FormatRecords' >> beam.Map(format_record)
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
    #parser.add_argument('--project')
    parser.add_argument('--gcs_url')
    parser.add_argument('--dataset')
    parser.add_argument('--table')
    parser.add_argument('--layer_name')
    parser.add_argument('--in_epsg', type=int, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    create_table(known_args,pipeline_args)
    run(pipeline_args, known_args)
