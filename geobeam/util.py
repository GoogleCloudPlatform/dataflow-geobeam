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
This module contains utility functions that make working with geosaptial
data in Google Cloud easier.
"""

BQ_FIELD_TYPES = {
    'int': 'INT64',
    'str': 'STRING',
    'float': 'FLOAT64',
    'bool': 'BOOL',
    'date': 'DATE',
    'time': 'TIME',
    'datetime': 'DATETIME',
    'bytes': 'BYTES'
}


def get_bigquery_raster_schema(band_column='value', band_type='INT64'):
    """
    Generate Bigquery table schema for a raster
    """
    return [
        {
            'name': band_column,
            'type': band_type
        },
        {
            'name': 'geom',
            'type': 'GEOGRAPHY'
        }
    ]


def get_bigquery_schema(filepath, layer_name=None, gdb_name=None):
    """
    Generate a Bigquery table schema from a geospatial file

        python -m geobeam.util get_bigquery_schema ...args

    Args:
        filepath (str): full path to the input file
        layer_name (str, optional): name of the layer, if file contains
            multiple layers
    Returns:
        dict: the schema, convertable to json by json.dumps(schema, indent=2)
    """

    import fiona
    from fiona.io import ZipMemoryFile
    from fiona import prop_type

    bq_schema = []

    if filepath.endswith('.zip') and layer_name is None:
        profile = fiona.open('zip://' + filepath).profile
    elif filepath.endswith('.zip'):
        profile = fiona.open('zip://' + filepath, layer=layer_name).profile
    elif gdb_name is not None:
        f = open(filepath, 'rb')
        mem = ZipMemoryFile(f.read())
        profile = mem.open(gdb_name, layer=layer_name).profile
    else:
        profile = fiona.open(filepath, layer=layer_name).profile

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
        'description': '{} loaded from {}'.format(profile['schema']['geometry'], profile['driver'])
    })

    return bq_schema

def get_bigquery_schema_dataflow(filepath, layer_name=None, gdb_name=None):
    """
    Generate a Bigquery table schema from a geospatial file hosted on a Google Cloud Storage bucket

        from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

        table_schema = parse_table_schema_from_json(get_bigquery_schema_dataflow(known_args.gcs_url, known_args.layer_name))

    Args:
        filepath (str): full path to the input file hosted on Google Cloud Storage
        layer_name (str, optional): name of the layer, if file contains
            multiple layers
    Returns:
        JSON: the schema in JSON that can be passed to the schema argument in WriteToBigQuery.
        Must use the parse_table_schema_from_json() from apache_beam.io.gcp.bigquery_tools
    """

    from google.cloud import storage
    import fiona
    import json
    from fiona import BytesCollection

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

    if gdb_name is not None:
        with fiona.io.ZipMemoryFile(data) as zip:
            with zip.open(f'{zip_name}.gdb', layer=gdb_name) as collection:
                print(collection)
                profile = collection.profile
    elif layer_name is not None:
        profile = BytesCollection(data, layer=layer_name).profile
    else:
        profile = fiona.open(gcs_url).profile
            
    from fiona import prop_type

    BQ_FIELD_TYPES = {
        'int': 'INT64',
        'str': 'STRING',
        'float': 'FLOAT64',
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
        'description': '{} reprojected from {}. source: {}'.format(
            profile['schema']['geometry'], profile['crs']['init'], profile['driver'])
    })
    
    return json.JSONEncoder(sort_keys=True).encode({"fields": bq_schema})


#function read shapefile based on the layer submitted, derive schema and create BQ table if doesn't exist
#beam.io.WriteToBigQuery in run function for some reason struggles with the standard json schema definitions like {"NAME":"TYPE"} (sends it with escaped " and BQ isn't happy about it)

def create_table_from_shp(known_args,pipeline_args): 

    import fiona
    from fiona import BytesCollection
    import json
    
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    from google.cloud import storage

    from apache_beam.options.pipeline_options import PipelineOptions


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
            with zip.open(f'{layer_name}.shp') as collection:
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
   
    options = list(PipelineOptions(pipeline_args).display_data().values()) #impoassible to acquire project from known_args, had to be creative with PipelineOptions
    table_id=f"{options[1]}.{known_args.dataset}.{known_args.table}"
    
    try:
        client.get_table(table_id)  
        print("Table {} already exists.".format(table_id))
        #table = client.delete_table(table_id) #We are using WRITE_TRUNCATE in BigQuery, so no need to delete, if exists
    except NotFound:
         print("Table {} is not found. Creating.".format(table_id))
         bigquerySchema = []
         bigqueryColumns = json.loads(schema_json)
         for col in bigqueryColumns:
            bigquerySchema.append(bigquery.SchemaField(col['name'], col['type']))
            table = bigquery.Table(table_id, schema=bigquerySchema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
                )    
 


if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument('method', metavar='method', type=str)
    parser.add_argument('--file', type=str)
    parser.add_argument('--layer_name', type=str, default=None)
    parser.add_argument('--gdb_name', type=str, default=None)
    args, _ = parser.parse_known_args()

    if args.method == 'get_bigquery_schema':
        schema = get_bigquery_schema(args.file, args.layer_name, args.gdb_name)
        print(json.dumps(schema, indent=4))
