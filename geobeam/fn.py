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


def make_valid(element, drop_z=True):
    """
    Attempt to make a geometry valid. Returns `None` if the geometry cannot
    be made valid.

    Example:
    .. code-block:: python

        p | beam.Map(geobeam.fn.make_valid)
          | beam.Map(geobeam.fn.filter_invalid)
    """
    from shapely.geometry import shape
    from shapely import validation, wkb

    props, geom = element
    shape_geom = shape(geom)

    if not shape_geom.is_valid:
        shape_geom = validation.make_valid(shape_geom)

    if drop_z and shape_geom.has_z:
        shape_geom = wkb.loads(wkb.dumps(shape_geom, output_dimension=2))

    if shape_geom is not None:
        return (props, shape_geom.__geo_interface__)
    else:
        return None


def filter_invalid(element):
    """
    Use with fn.make_valid to filter out geometries that are invalid, empty,
    or are out of bounds.

    Example:
    .. code-block:: python

        p | beam.Map(geobeam.fn.make_valid)
          | beam.Map(geobeam.fn.filter_invalid)
    """

    from shapely.geometry import shape

    if element is None:
        return False

    props, geom = element

    if geom is None or geom.get('coordinates') is None:
        return False

    shape_geom = shape(geom)

    if not shape_geom.is_valid or shape_geom.is_empty:
        return False

    minx, miny, maxx, maxy = shape_geom.bounds

    if minx < -180 or miny < -90 or maxx > 180 or maxy > 90:
        return False

    return True


def trim_polygons(element, d=0.0000001, cf=1.2):
    """
    Remove extraneous artifacts, tails, etc. from otherwise valid polygons

    Args:
        d (float, optional): trim distance
        cf (float, optional): corrective factor

    Exmaple:
    .. code-block:: python

        p | beam.Map(geobeam.fn.trim_polygons, d=0.00001, cf=1.2
    """

    from shapely.geometry import shape
    props, geom = element

    shape_geom = shape(geom)

    if shape_geom.type not in ['Polygon', 'MultiPolygon']:
        return (props, geom)

    return (
        props,
        shape_geom
            .buffer(-d)
            .buffer(d * cf)
            .intersection(shape_geom)
            .simplify(d)
            .__geo_interface__
    )


def format_record(element, band_column=None, band_type='int'):
    """
    Format the tuple received from the geobeam file source into a record
    that can be inserted into BigQuery. If using a raster source, the
    bands and band_column will be combined.

    Args:
        band_column (str, optional): the name of the raster band column
        band_type (str, optional): Default to int. The data type of the
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
