"""
Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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


def get_bigquery_schema(filepath, layer_name=None):
    """Generate a Bigquery table schema from a geospatial file

    Args:
        filepath (str): full path to the input file
        layer_name (str, optional): name of the layer, if file contains
            multiple layers
    Returns:
        dict: the schema, convertable to json by json.dumps(schema, indent=2)
    """

    import fiona
    from fiona import prop_type

    bq_schema = []
    profile = fiona.open(filepath).profile

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

    return bq_schema


if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str)
    parser.add_argument('--layer_name', type=str, default=None)
    args, _ = parser.parse_known_args()

    schema = get_bigquery_schema(args.file, args.layer_name)
    print(json.dumps(schema, indent=2))
