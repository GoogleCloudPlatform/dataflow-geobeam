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
    Attempt to make a geometry valid. Returns `None` if the geometry cannot
    be made valid.

    Example:
    .. code-block:: python

        p | beam.Map(geobeam.fn.make_valid)
          | beam.Map(geobeam.fn.filter_invalid)
    """
    from shapely.geometry import shape

    props, geom = element

    geom = shape(geom).buffer(0)

    if geom.is_valid:
        return (props, geom.__geo_interface__)
    else:
        return None


def filter_invalid(element):
    from shapely.geometry import shape

    """
    Use with fn.make_valid to filter out geometries that are invalid.

    Example:
    .. code-block:: python

        p | beam.Map(geobeam.fn.make_valid)
          | beam.Map(geobeam.fn.filter_invalid)
    """

    if element is None:
        return False

    props, geom = element

    if geom is None:
        return False

    return shape(geom).is_valid


def format_record(element, band_column=None, band_type='int'):
    """
    Format the tuple received from the geobeam file source into a record
    that can be inserted into BigQuery. If using a raster source, the
    bands and band_column will be combined.

    Args:
        band_column (str, optional): the name of the raster band column
        band_type (type, optional): Default to int. The data type of the
            raster band column to store in the database.

    Example:
    .. code-block:: python

        # vector
        p | beam.Map(geobeam.fn.format_record)

        # raster
        p | beam.Map(geobeam.fn.format_record,
            band_column='elev', band_type=float)
    """
    import json

    props, geom = element
    cast = eval(band_type)

    if band_column and band_type:
        return {
            band_column: cast(props),
            'geom': json.dumps(geom)
        }
    else:
        return {
            **props,
            'geom': json.dumps(geom)
        }
