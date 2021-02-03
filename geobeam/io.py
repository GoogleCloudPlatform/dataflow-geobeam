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


class GeotiffSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading Geotiff files.

    The Geotiff is read in blocks and each block is polygonized. Each polygon
    is returned as a (`value`, `geom`) tuple, where `value` is the band value
    of the polygonized pixels, and `geom` is the Polygon (or Point if
    centroid_only is True) geometry that corresponds to the `value`. The raster
    is stored in RAM, so make sure you specify a machine_type with enough RAM
    to hold the entire raster image.

        p | beam.io.Read(GeotiffSource(file_pattern))
          | beam.Map(print)

    Args:
        file_pattern (str): required, passed to FileBasedSource.
        band_number (int, optional): Defaults to `1`. the band(s) to read from
            the raster.
        include_nodata (bool, optional): Defaults to `False`. False to ignore
            nodata values in the raster band; True to include them.
        centroid_only (bool, optional): Defaults to `False`. True to set geom
            to the centroid Point, False to include the entire Polygon. Do not
            use with the merge_blocks option.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
            This can be useful if your data is in a bespoke CRS that requires a
            custom reprojection, or if you want to join/clip with other spatial
            data in the same projection. Note: you will need to manually
            reproject all geometries to EPSG:4326 in order to store it in
            BigQuery.
        in_epsg (int, optional): override the source projection of your input
            raster in case its missing or incorrect
        merge_blocks (int, optional): Defaults to `32`. Number of windows
            to combine during polygonization. Setting this to a larger number
            will result in fewer file reads and possible improved overall
            performance. Setting this value too high (>100) may cause file
            read issues and worker timeouts. Set to a smaller number if your
            raster blocks are large (256x256 or larger). Do not use with the
            centroid_only option.

    Yields:
        generator of (`value`, `geom`) tuples. The data type of `value` is
        determined by the raster band it came from.

    """

    def read_records(self, file_name, range_tracker):
        from rasterio.io import MemoryFile
        from rasterio.features import shapes
        from rasterio.warp import calculate_default_transform
        from rasterio.windows import bounds, union
        from shapely.geometry import Point, shape
        from fiona import transform
        import json
        import numpy

        total_bytes = self.estimate_size()

        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        with self.open_file(file_name) as f, MemoryFile(f.read()) as m, m.open() as src:
            is_wgs84, src_crs = _GeoSourceUtils.validate_crs(
                    src.crs, self.in_epsg)

            block_windows = list([win for ji, win in src.block_windows()])
            num_windows = len(block_windows)
            window_bytes = math.floor(total_bytes / num_windows)
            i = 0

            logging.info(json.dumps({
                'msg': 'read_records',
                'file_name': file_name,
                'file_size_mb': round(total_bytes / 1048576),
                'block_windows': num_windows,
                'merge_blocks': self.merge_blocks,
                'include_nodata': self.include_nodata,
                'src_profile': src.profile
            }, default=str))

            while range_tracker.try_claim(next_pos):
                i = math.ceil(next_pos / window_bytes)
                if i >= num_windows:
                    break

                slice_end = min(i + self.merge_blocks, num_windows)
                window_slice = block_windows[i:slice_end]
                cur_window = union(window_slice)
                #cur_window = block_windows[i]
                cur_transform = src.window_transform(cur_window)
                win_bounds = bounds(cur_window, cur_transform)

                if self.include_nodata:
                    block = src.read(self.band_number, window=cur_window)
                else:
                    block = src.read(self.band_number, window=cur_window, masked=True)


                logging.debug(json.dumps({
                    'msg': 'read_records.try_claim',
                    'file_name': file_name,
                    'band_number': self.band_number,
                    'next_pos': next_pos,
                    'i': i,
                    'window': cur_window
                }, default=str))

                if self.centroid_only:
                    wgs84, w, h = calculate_default_transform(src_crs,
                        'epsg:4326', cur_window.width, cur_window.height,
                        *win_bounds)

                    for j, i in numpy.ndindex(block.shape):
                        x, y = ((i + 0.5), (j - 0.5)) * wgs84

                        yield (block[j][i], Point(x, y).__geo_interface__)

                else:
                    for (g, v) in shapes(block, transform=cur_transform):
                        if not is_wgs84 or self.skip_reproject:
                            geom = transform.transform_geom(src_crs, 'epsg:4326', g)
                        else:
                            geom = g

                        yield (v, geom)

                next_pos = next_pos + (window_bytes * self.merge_blocks)
                #next_pos = next_pos + window_bytes

    def __init__(self, file_pattern, band_number=1, include_nodata=False,
                 skip_reproject=False, centroid_only=False, in_epsg=None,
                 merge_blocks=16, **kwargs):
        self.include_nodata = include_nodata
        self.skip_reproject = skip_reproject
        self.centroid_only = centroid_only
        self.in_epsg = in_epsg
        self.band_number = band_number

        if merge_blocks > 10000 or merge_blocks < 1:
            raise Exception('merge_blocks option must be 1 >= x < 10000')

        self.merge_blocks = merge_blocks

        super(GeotiffSource, self).__init__(file_pattern)


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
        in_epsg (int, optional): override the source projection of your input
            shapefile in case of a missing or incorrect .prj file

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

            src_crs = _GeoSourceUtils.validate_crs(collection.crs, self.in_epsg)

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

    def __init__(self, file_pattern, layer_name=None, skip_reproject=False,
                 in_epsg=None, **kwargs):

        self.layer_name = layer_name
        self.skip_reproject = skip_reproject
        self.in_epsg = in_epsg

        super(ShapefileSource, self).__init__(file_pattern)


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
        in_epsg (int, optional): override the source projection of your input
            shapefile in case of a missing or incorrect CRS

    Yields:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    """

    def read_records(self, file_name, range_tracker):
        from fiona import transform
        from fiona.io import ZipMemoryFile
        import json

        total_bytes = self.estimate_size()
        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        with self.open_file(file_name) as f, ZipMemoryFile(f.read()) as mem:
            collection = mem.open(self.gdb_name, layer=self.layer_name)
            src_crs = _GeoSourceUtils.validate_crs(collection.crs, self.in_epsg)

            num_features = len(collection)
            feature_bytes = math.floor(total_bytes / num_features)
            i = 0

            # XXX workaround due to https://github.com/Toblerity/Fiona/issues/996
            features = list(collection)

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

                cur_feature = features[i]
                geom = cur_feature['geometry']
                props = cur_feature['properties']

                if not self.skip_reproject:
                    geom = transform.transform_geom(src_crs, 'epsg:4326', geom)

                yield (props, geom)

                next_pos = next_pos + feature_bytes

    def __init__(self, file_pattern, gdb_name=None, layer_name=None,
            in_epsg=None, skip_reproject=False, **kwargs):

        self.gdb_name = gdb_name
        self.layer_name = layer_name
        self.skip_reproject = skip_reproject
        self.in_epsg = in_epsg

        super(GeodatabaseSource, self).__init__(file_pattern)


class _GeoSourceUtils():
    """Utility methods for the FileBasedSource reader classes"""

    @staticmethod
    def validate_crs(_src_crs, in_epsg):
        from fiona import crs

        src_crs = _src_crs.to_dict()

        if in_epsg is not None:
            in_crs = crs.from_epsg(in_epsg)

            if bool(src_crs) is True:
                logging.warning('manually specified CRS {} is being used instead of raster CRS {}'.format(
                    in_crs, src_crs))

            return in_crs

        if bool(src_crs) is False:
            logging.error('--in_epsg must be specified because raster CRS is empty')
            raise Exception()

        is_wgs84 = _src_crs.to_epsg() == 4326

        return is_wgs84, src_crs
