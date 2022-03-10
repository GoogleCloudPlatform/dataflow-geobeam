# Copyright 2022 Google LLC
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
Example pipeline that reads messages that contain geospatial coordinates
from a Pubsub topic and reprojects. Useful for streaming data from connected
vehicles.
"""

from apache_beam import DoFn

class HandleMessage(DoFn):
    def __init__(self, in_proj):
        from fiona import crs
        self.in_crs = crs.from_string(in_proj)

    def process(self, element: bytes):
        import json
        from fiona.transform import transform_geom

        parsed = json.loads(element.decode('utf-8'))
        geom = {
            "type": "Point",
            "coordinates": [parsed['x'], parsed['y']]
        }

        # reproject from the given proj str to 4326, aka WGS 84 used by BQ
        geom = transform_geom(self.in_crs, 'epsg:4326', geom)

        # ready to be inserted into BQ
        record = {
            **parsed,
            'geom': json.dumps(geom)
        }
        logging.info(record)
        yield record

def run(pipeline_args, known_args):

    import apache_beam as beam
    from apache_beam import io
    from apache_beam.options.pipeline_options import PipelineOptions

    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
    ] + pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | io.ReadFromPubSub(topic='projects/example-project/topics/example-topic')
         | beam.ParDo(HandleMessage(known_args.in_proj)))


if __name__ == '__main__':
    import logging
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--in_proj', type=str, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)
