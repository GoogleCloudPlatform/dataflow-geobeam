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
This package contains Apache Beam I/O connectors for reading from spatial data
files.
"""

import math
import logging

from apache_beam.io import iobase
from apache_beam.io import filebasedsource


class RasterBlockSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading pixel blocks from raster files in
    equally-sized blocks.

    The raster file is read in MxN blocks, and each band is read as a MxN
    array. M and N are determined by the input raster file and are not
    runtime configurable.

    `RasterBlockSource` optimizes for pipeline speed, and is best for ELT
    processes where further processing can be done by the sink (e.g.
    BigQuery).

        p | beam.io.Read(RasterBlockSource(file_pattern))
          | beam.Map(print)

    Args:
        file_pattern (str): required, passed to FileBasedSource.
        bidx (int[], optional): Defaults to `1`. The band indexes to read from
            the raster.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
            This can be useful if your data is in a bespoke CRS that requires a
            custom reprojection, or if you want to join/clip with other spatial
            data in the same projection. Note: you will need to manually
            reproject all geometries to EPSG:4326 in order to store it in
            BigQuery.
        in_epsg (int, optional): override the source projection with an EPSG
            code.
        in_proj (str, optional): override the source projection with a
            PROJ4 string.

    Yields:
        generator of (`value`, `geom`) tuples. `value` is a 3D
        array; the first dimension is the band index, and the remaining two
        dimensions represent the pixel values for the band.
    """

    def read_records(self, file_name, range_tracker):
        import rasterio
        from fiona.transform import transform_geom
        import json

        total_bytes = self.estimate_size()

        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        with rasterio.open(file_name) as src:
            is_wgs84, src_crs = _GeoSourceUtils.validate_crs(src.crs, self.in_epsg, self.in_proj)

            block_windows = list([win for ji, win in src.block_windows()])
            num_windows = len(block_windows)
            window_bytes = math.floor(total_bytes / num_windows)
            i = 0

            logging.info(json.dumps({
                'msg': 'read_records',
                'next_pos': next_pos,
                'file_name': file_name,
                'file_size_mb': round(total_bytes / 1048576),
                'src_profile': src.profile,
                'total_bytes': total_bytes,
                'window_bytes': window_bytes,
                'num_windows': num_windows
            }, default=str))

            while range_tracker.try_claim(next_pos):
                i = math.ceil(next_pos / window_bytes)
                if i >= num_windows:
                    break

                win = block_windows[i]

                logging.info(json.dumps({
                    'msg': 'read_records.try_claim',
                    'file_name': file_name,
                    'next_pos': next_pos,
                    'i': i,
                    'window': win
                }, default=str))

                xfrm = src.window_transform(win)

                exterior_ring = [
                    xfrm * (0, 0),
                    xfrm * (0, -win.height),
                    xfrm * (win.width, -win.height),
                    xfrm * (win.width, 0),
                    xfrm * (0, 0)
                ]
                geom_obj = {
                    'type': 'Polygon',
                    'coordinates': [ exterior_ring ]
                }

                if self.skip_reproject:
                    geom = geom_obj
                else:
                    geom = transform_geom(src_crs, 'epsg:4326', geom_obj)

                if self.bidx:
                    block = src.read(self.bidx, window=win)
                else:
                    block = src.read(window=win)

                yield (block, geom)

                next_pos = next_pos + window_bytes


    def __init__(self, file_pattern, bidx=None, in_epsg=None, in_proj=None,
                 skip_reproject=False, **kwargs):
        self.in_epsg = in_epsg
        self.in_proj = in_proj
        self.bidx = bidx
        self.skip_reproject = skip_reproject

        super(RasterBlockSource, self).__init__(file_pattern)

class RasterPolygonSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading pixels grouped by value from raster
    files.

    The raster file is read in blocks and each block is polygonized. Each
    polygon is returned as a (`value`, `geom`) tuple, where `value` is the band
    value of the polygonized pixels, and `geom` is the Polygon (or Point if
    centroid_only is True) geometry that corresponds to the `value`.

    `RasterPolygonSource` optimizes for immediate usability of the output, and so
    is most suitable for ETL processes; it will be slower than RasterBlockSource,
    produce more rows of output, and can only read one band at a time.

        p | beam.io.Read(RasterPolygonSource(file_pattern))
          | beam.Map(print)

    Args:
        file_pattern (str): required, passed to FileBasedSource.
        bidx (int, optional): Defaults to `1`. The band index to read from
            the raster.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
            This can be useful if your data is in a bespoke CRS that requires a
            custom reprojection, or if you want to join/clip with other spatial
            data in the same projection. Note: you will need to manually
            reproject all geometries to EPSG:4326 in order to store it in
            BigQuery.
        in_epsg (int, optional): override the source projection with an EPSG
            code.
        in_proj (str, optional): override the source projection with a
            PROJ4 string.

    Yields:
        generator of (`value`, `geom`) tuples. The data type of `value` is
        determined by the raster band it came from.
    """

    def read_records(self, file_name, range_tracker):
        import rasterio
        from rasterio.features import shapes
        from fiona.transform import transform_geom
        import json

        total_bytes = self.estimate_size()

        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        with rasterio.open(file_name) as src:
            is_wgs84, src_crs = _GeoSourceUtils.validate_crs(src.crs, self.in_epsg, self.in_proj)

            block_windows = list([win for ji, win in src.block_windows()])
            num_windows = len(block_windows)
            window_bytes = math.floor(total_bytes / num_windows)
            i = 0

            logging.info(json.dumps({
                'msg': 'read_records',
                'next_pos': next_pos,
                'file_name': file_name,
                'file_size_mb': round(total_bytes / 1048576),
                'block_windows': num_windows,
                'src_profile': src.profile
            }, default=str))

            while range_tracker.try_claim(next_pos):
                i = math.ceil(next_pos / window_bytes)
                if i >= num_windows:
                    break

                win = block_windows[i]
                block = src.read(self.bidx, window=win)
                block_mask = src.read_masks(self.bidx, window=win)
                xfm = src.window_transform(win)

                logging.info(json.dumps({
                    'msg': 'read_records.try_claim',
                    'file_name': file_name,
                    'bidx': self.bidx,
                    'i': i,
                    'window': win
                }, default=str))

                for (g, v) in shapes(block, block_mask, transform=xfm):
                    geom = transform_geom(src_crs, 'epsg:4326', g)
                    yield (v, geom)

                next_pos = next_pos + window_bytes

    def __init__(self, file_pattern, bidx=1, in_epsg=None, in_proj=None, **kwargs):
        self.in_epsg = in_epsg
        self.in_proj = in_proj
        self.bidx = bidx

        super(RasterPolygonSource, self).__init__(file_pattern)


class ShapefileSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading shapefiles.

    The given file(s) should be a zip archive containing the .shp file
    alongside the .dbf and .prj files.

        p | beam.io.Read(ShapefileSource(file_pattern))
          | beam.Map(print)

    Args:
        layer_name (str, optional): the name of the layer you want to read.
            Required the zipfile contains multiple layers.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
        in_epsg (int, optional): override the source projection with an EPSG
            code.
        in_proj (str, optional): override the source projection with a
            PROJ4 string.

    Yields:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    """

    def read_records(self, file_name, range_tracker):
        from fiona import BytesCollection
        from fiona.transform import transform_geom
        import json

        total_bytes = self.estimate_size()
        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        with self.open_file(file_name) as f:
            if self.layer_name:
                collection = BytesCollection(f.read(), layer=self.layer_name)
            else:
                collection = BytesCollection(f.read())

            is_wgs84, src_crs = _GeoSourceUtils.validate_crs(collection.crs, self.in_epsg, self.in_proj)

            num_features = len(collection)
            feature_bytes = math.floor(total_bytes / num_features)
            i = 0

            logging.info(json.dumps({
                'msg': 'read_records',
                'next_pos': next_pos,
                'file_name': file_name,
                'profile': collection.profile,
                'num_features': num_features,
                'total_bytes': total_bytes
            }))

            while range_tracker.try_claim(next_pos):
                i = math.ceil(next_pos / feature_bytes)
                if i >= num_features:
                    break

                cur_feature = collection[i]
                geom = cur_feature['geometry']
                props = cur_feature['properties']

                if not self.skip_reproject:
                    geom = transform_geom(src_crs, 'epsg:4326', geom)

                yield (props, geom)

                next_pos = next_pos + feature_bytes

    def __init__(self, file_pattern, layer_name=None, skip_reproject=False,
                 in_epsg=None, in_proj=None,**kwargs):

        self.layer_name = layer_name
        self.skip_reproject = skip_reproject
        self.in_epsg = in_epsg
        self.in_proj = in_proj

        super(ShapefileSource, self).__init__(file_pattern, min_bundle_size=int(1e9))


class GeodatabaseSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading geodatabases.

    The given file(s) should be a zip archive containing .gdb geodatabase
    directory.

        p | beam.io.Read(GeodatabaseSource(file_pattern))
          | beam.Map(print)

    Args:
        gdb_name (str): Required. the name of the .gdb directory in the archive,
            e.g. `FRD_510104_Coastal_GeoDatabase_20160708.gdb`
        layer_name (str): Required. the name of the layer you want to read from
            the gdb, e.g. `S_CSLF_Ar`
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
        in_epsg (int, optional): override the source projection with an EPSG
            code.
        in_proj (str, optional): override the source projection with a
            PROJ4 string.

    Yields:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    """
    def read_records(self, file_name, range_tracker):
        import fiona
        from fiona import transform
        import json

        fiona_path = 'zip+{}/{}'.format(file_name, self.gdb_name)

        total_bytes = self.estimate_size()
        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        gdb_layers = fiona.listlayers(fiona_path)
        if self.layer_name and self.layer_name not in gdb_layers:
            logging.warning(json.dumps({
                'msg': 'gdb_layer_not_found',
                'layer_name': self.layer_name,
                'gdb_name': self.gdb_name,
                'gdb_layers': gdb_layers
            }))
            self._pattern = '/dev/null'
            return

        with fiona.open(fiona_path, layer=self.layer_name) as collection:
            is_wgs84, src_crs = _GeoSourceUtils.validate_crs(collection.crs, self.in_epsg, self.in_proj)

            num_features = len(collection)
            feature_bytes = math.floor(total_bytes / num_features)
            i = 0

            logging.info(json.dumps({
                'msg': 'read_records',
                'next_pos': next_pos,
                'file_name': file_name,
                'layer_name': self.layer_name,
                'profile': collection.profile,
                'num_features': num_features,
                'total_bytes': total_bytes
            }))

            while range_tracker.try_claim(next_pos):
                i = math.ceil(next_pos / feature_bytes)
                if i >= num_features:
                    break

                next_pos = next_pos + feature_bytes

                cur_feature = collection[i]
                if cur_feature is None:
                    continue

                geom = cur_feature['geometry']

                if geom is None:
                    logging.info('Skipping null geometry: {}'.format(cur_feature))
                    continue

                if not geom['coordinates']:
                    logging.info('Skipping empty geometry: {}'.format(cur_feature))
                    continue

                if not self.skip_reproject and not is_wgs84:
                    geom = transform.transform_geom(src_crs, 'epsg:4326', geom)

                yield (cur_feature['properties'], geom)


    def __init__(self, file_pattern, gdb_name=None, layer_name=None,
            in_epsg=None, in_proj=None, skip_reproject=False, **kwargs):

        self.gdb_name = gdb_name
        self.layer_name = layer_name
        self.skip_reproject = skip_reproject
        self.in_epsg = in_epsg
        self.in_proj = in_proj

        super(GeodatabaseSource, self).__init__(file_pattern, min_bundle_size=int(1e9))


class GeoJSONSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading GeoJSON Files.

    The given file(s) should be a .geojson file.

        p | beam.io.Read(GeoJSONSource(file_pattern))
          | beam.Map(print)

    Args:
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
        in_epsg (int, optional): override the source projection with an EPSG
            code.
        in_proj (str, optional): override the source projection with a
            PROJ4 string.

    Yields:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    """

    def read_records(self, file_name, range_tracker):
        import fiona
        from fiona.transform import transform_geom
        import json

        total_bytes = self.estimate_size()
        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        collection = fiona.open(file_name)
        is_wgs84, src_crs = _GeoSourceUtils.validate_crs(collection.crs, self.in_epsg, self.in_proj)

        num_features = len(collection)
        feature_bytes = math.floor(total_bytes / num_features)
        i = 0

        logging.info(json.dumps({
            'msg': 'read_records',
            'file_name': file_name,
            'profile': collection.profile,
            'num_features': num_features,
            'total_bytes': total_bytes
        }))

        while range_tracker.try_claim(next_pos):
            i = math.ceil(next_pos / feature_bytes)
            if i >= num_features:
                break

            cur_feature = collection[i]
            geom = cur_feature['geometry']
            props = cur_feature['properties']

            if not self.skip_reproject:
                geom = transform_geom(src_crs, 'epsg:4326', geom)

            yield (props, geom)

            next_pos = next_pos + feature_bytes

    def __init__(self, file_pattern, skip_reproject=False,
                 in_epsg=None, in_proj=None, **kwargs):

        self.skip_reproject = skip_reproject
        self.in_epsg = in_epsg
        self.in_proj = in_proj

        super(GeoJSONSource, self).__init__(file_pattern)


class ESRIServerSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading layers from an ESRI ArcGIS Server.

    The given file(s) should be a link to a specific layer from the ArcGIS Server REST API
    (ex. https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_States_Generalized/FeatureServer/0)

        p | beam.io.Read(ESRIServerSource(file_pattern))
          | beam.Map(print)

    Args:
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
        in_epsg (int, optional): override the source projection with an EPSG
            code.
        in_proj (str, optional): override the source projection with a
            PROJ4 string.

    Yields:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    """

    def read_records(self, file_name, range_tracker):
        from fiona import BytesCollection
        from fiona.transform import transform_geom
        import json
        from esridump.dumper import EsriDumper

        total_bytes = self.estimate_size()
        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        esri_dump = EsriDumper(file_name)

        geojson = {
        "type": "FeatureCollection",
        "features": list(esri_dump) }

        collection = BytesCollection(json.dumps(geojson, indent=2).encode('utf-8'))
        is_wgs84, src_crs = _GeoSourceUtils.validate_crs(collection.crs, self.in_epsg, self.in_proj)

        num_features = len(collection)
        feature_bytes = math.floor(total_bytes / num_features)
        i = 0

        logging.info(json.dumps({
            'msg': 'read_records',
            'file_name': file_name,
            'profile': collection.profile,
            'num_features': num_features,
            'total_bytes': total_bytes
        }))

        while range_tracker.try_claim(next_pos):
            i = math.ceil(next_pos / feature_bytes)
            if i >= num_features:
                break

            cur_feature = collection[i]
            geom = cur_feature['geometry']
            props = cur_feature['properties']

            if not self.skip_reproject:
                geom = transform_geom(src_crs, 'epsg:4326', geom)

            yield (props, geom)

            next_pos = next_pos + feature_bytes

    def __init__(self, file_pattern, skip_reproject=False,
                 in_epsg=None, in_proj=None, **kwargs):

        self.skip_reproject = skip_reproject
        self.in_epsg = in_epsg
        self.in_proj = in_proj

        super(ESRIServerSource, self).__init__(file_pattern)


class _GeoSourceUtils():
    """Utility methods for the FileBasedSource reader classes"""

    @staticmethod
    def validate_crs(_src_crs, in_epsg, in_proj=None):
        from fiona import crs

        if type(_src_crs) == dict:
            src_crs = _src_crs
            is_wgs84 = 'init' in src_crs and (src_crs['init'] == 'epsg:4326')
        else:
            src_crs = _src_crs.to_dict()
            is_wgs84 = _src_crs.to_epsg() == 4326

        if in_epsg is not None:
            if in_proj is not None:
                logging.error('--in_epsg and --in_proj cannot both be set. use one or the other.')
                raise Exception()

            in_crs = crs.from_epsg(in_epsg)

            if bool(src_crs) is True:
                logging.warning('manually specified CRS {} is being used instead of detected CRS {}.'.format(
                    in_crs, src_crs))

            return in_crs

        if in_proj is not None:
            in_crs = crs.from_string(in_proj)

            if bool(src_crs) is True:
                logging.warning('manually specified CRS {} is being used instead of raster CRS {}.'.format(
                    in_crs, src_crs))

            return in_crs

        if bool(src_crs) is False:
            logging.error('--in_epsg must be specified because raster CRS is empty.')
            raise Exception()

        return is_wgs84, src_crs
