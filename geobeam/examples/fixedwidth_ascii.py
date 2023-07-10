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
Example that reads XYZ coordinates from a fixed-width text file
"""

def slice_fw_row(row, column_widths):
    record = {}
    for column, [start, end] in column_widths.items():
        record[column] = row[start:end].strip()

    return record


def cast_field_values(record):
    # cast to correct type
    # harcoded here -- can be made dynamic based on schema

    record['x'] = float(record['x'])
    record['y'] = float(record['y'])
    record['track'] = int(record['track'])
    record['bin'] = float(record['bin'])
    record['z'] = float(record['z'])
    record['amplitude'] = int(record['amplitude'])

    return record


def enrich_with_point(record, in_crs):
        import json
        from fiona.transform import transform

        # reproject from the given proj str to 4326, aka WGS 84 used by BQ
        ([x], [y]) = transform(in_crs, 'epsg:4326', [record['x']], [record['y']])

        return {
            **record,
            'geom': json.dumps({ 'type': 'Point', 'coordinates': (x, y) })
        }


def run(pipeline_args, known_args):

    import apache_beam as beam
    from apache_beam import io
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery

    # could build this dynamically for each file or accept as arguments
    file_metadata = {
        'columns': {
            'x': [1, 14],
            'y': [15, 30],
            'track': [31, 46],
            'bin': [47, 59],
            'z': [60, 69],
            'amplitude': [70, 71]
        },
        'epsg': '26715'
    }

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        (p
         | io.textio.ReadFromText(known_args.in_file)
         | beam.Map(slice_fw_row, file_metadata['columns'])
         | beam.Map(cast_field_values)
         | beam.Map(enrich_with_point, known_args.in_crs)
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 projectId='dataflow-geobeam',
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
    parser.add_argument('--in_file', type=str, default=None)
    parser.add_argument('--in_crs', type=str, default=None)
    parser.add_argument('--dataset', type=str, default=None)
    parser.add_argument('--table', type=str, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
