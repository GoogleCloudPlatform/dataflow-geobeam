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
Beam functions, transforms, and filters that can be used to process
geometries in your pipeline
"""

def make_valid(element):
    """
    Runs the OGR MakeValid function on a geometry, and returns a new, valid
    geometry in a tuple (props, geom). Returns `None` if the geometry cannot
    be made valid.

    Example:
    ```
        p | beam.Map(geobeam.fn.make_valid)
          | beam.Map(geobeam.fn.filter_invalid)
    ```
    """
    from osgeo import ogr
    import json

    props, geom = element
    geom = ogr.CreateGeometryFromJson(json.dumps(geom))
    valid_geom = geom.MakeValid()

    if valid_geom is None:
        return None

    return (props, json.loads(valid_geom.ExportToJson()))


def filter_invalid(element):
    from shapely.geometry import shape

    """
    Use with fn.make_valid to filter out geometries that are invalid.

    Example:

         p | beam.Map(geobeam.fn.filter_invalid)
    """

    props, geom = element

    return shape(geom).is_valid


def format_record(element, band_column=None):
    """
    Format the tuple received from the geobeam file source into a record
    that can be inserted into BigQuery. If using a raster source, the
    bands and band_column will be combined.

    Example:
    ```
        # vector
        p | beam.Map(geobeam.fn.format_record)

        # raster
        p | beam.Map(geobeam.fn.format_record, band_column='elev')
    ```
    """
    props, geom = element

    if band_column:
        return { band_column: props, 'geom': geom }
    else:
        return { **props, 'geom': geom }
