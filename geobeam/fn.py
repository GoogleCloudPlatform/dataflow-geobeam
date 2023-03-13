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

import apache_beam as beam

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


def format_rasterblock_record(element, band_mapping=None):
    """
    Format the tuple received from a RasterBlockSource into a
    record that can be inserted into BigQuery or another database.

    Args:
        band_mapping (dict, optional): a band number to band name
        mapping used to name the output columns. band numbers are
        1-indexed, meaning they begin at 1 (by GDAL convention)

    Example:

    .. code-block:: python

        from geobeam.fn import format_rasterblock_record
        band_mapping = {
            1: 'elevation',
            4: 'red',
            5: 'green'
        }
        p | beam.Map(format_rasterblock_record, band_mapping=band_mapping)
    """

    import json
    from shapely.geometry import shape

    data, geom = element
    record = {}

    if band_mapping is None:
        for bidx in range(0, len(data)):
            band_data = data[bidx].tolist()
            record['band_{}'.format(bidx + 1)] = band_data
    else:
        for band in band_mapping:
            bidx = band - 1
            band_data = data[bidx].tolist()
            record[band_mapping[band]] = band_data

    return {
        **record,
        'geom': json.dumps(shape(geom).__geo_interface__)
    }


def format_rasterpolygon_record(element, band_type='int', band_column=None):
    """Format the tuple received from the geobeam raster source into a record
    that can be inserted into BigQuery or another database.

    Args:
        band_type (str, optional): Default to int. The data type of the
            raster band column to store in the database.
        band_column (str, optional): the name of the raster band column

    Example:
    .. code-block:: python

        beam.Map(geobeam.fn.format_record, band_column='elev', band_type=float)
    """
    import json
    from shapely.geometry import shape

    props, geom = element
    cast = eval(band_type)

    return {
        band_column: cast(props),
        'geom': json.dumps(shape(geom).__geo_interface__)
    }


def format_record(element):
    """Format the tuple received from the geobeam file source into a record
    that can be inserted into BigQuery or another database.

    Example:
    .. code-block:: python

        # vector
        p | beam.Map(geobeam.fn.format_record)
    """
    import json
    from shapely.geometry import shape

    props, geom = element

    return {
        **props,
        'geom': json.dumps(shape(geom).__geo_interface__)
    }


class DoBlockToPixelExterior(beam.DoFn):
    def process(self, element):
        """Decompose a raster block into individual pixels in order to store one
        pixel per row"""

        #import json
        #from shapely.geometry import shape
        from fiona.transform import transform_geom

        block_data, geom, xfrm, width, height, src_crs = element

        for i in range(0, height):
            for j in range(0, width):
                exterior_ring = pixel_to_ring(i, j, xfrm)

                geom_obj = {
                    'type': 'Polygon',
                    'coordinates': [ exterior_ring ]
                }
                geom = transform_geom(src_crs, 'epsg:4326', geom_obj)
                pixel_data = []

                for bidx in range(0, len(block_data)):
                    pixel_data.append(block_data[bidx][i][j])

                yield (pixel_data, geom)


def pixel_to_ring(i, j, xfrm):
    return [
        xfrm * (i, j),
        xfrm * (i, j + 1),
        xfrm * (i + 1, j + 1),
        xfrm * (i + 1, j),
        xfrm * (i, j)
    ]


def format_rasterpixel_record(element, band_mapping=None):
    import json
    from shapely.geometry import shape

    data, geom = element
    record = {}

    if band_mapping is None:
        for bidx in range(0, len(data)):
            band_data = data[bidx].tolist()
            record['band_{}'.format(bidx + 1)] = band_data
    else:
        for band in band_mapping:
            bidx = band - 1
            band_data = data[bidx]
            record[band_mapping[band]] = band_data

    return {
        **record,
        'geom': json.dumps(shape(geom).__geo_interface__)
    }
