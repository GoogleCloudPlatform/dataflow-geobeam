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

    Args:
        file_pattern (str): required, passed to FileBasedSource.
        band_number (int, optional): Defaults to `1`. the band to read from
            the raster.
        skip_nodata (bool, optional): Defaults to `True`. True to ignore
            nodata values in the raster band; False to include them.
        centroid_only (bool, optional): Defaults to `False`. True to set geom
            to the centroid Point, False to include the entire Polygon.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.
            This can be useful if your data is in a bespoke CRS that requires a
            custom reprojection, or if you want to join/clip with other spatial
            data in the same projection. Note: you will need to manually
            reproject all geometries to EPSG:4326 in order to store it in
            BigQuery.

    Returns:
        generator of (`value`, `geom`) tuples. The data type of `value` is
        determined by the raster band it came from.

    Example:
        p | beam.io.Read(GeotiffSource(file_pattern))
    """

    def read_records(self, file_name, range_tracker):
        from rasterio.io import MemoryFile
        from rasterio.features import shapes
        from shapely.geometry import shape
        from fiona import transform
        from numpy import dtype
        import json

        total_bytes = self.estimate_size()

        next_pos = range_tracker.start_position()

        def split_points_unclaimed(stop_pos):
            return 0 if stop_pos <= next_pos else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        with self.open_file(file_name) as f, MemoryFile(f.read()) as m:
            with m.open() as src:
                src_dtype = src.profile['dtype']
                src_crs = _GeoSourceUtils.validate_crs(src.crs.to_dict(), self.in_epsg)
                nodata_val = src.nodata

                block_windows = list([win for ji, win in src.block_windows()])
                num_windows = len(block_windows)
                window_bytes = math.floor(total_bytes / num_windows)
                i = 0

                logging.info(json.dumps({
                    'msg': 'read_records',
                    'file_name': file_name,
                    'file_size_mb': total_bytes / 1048576,
                    'src_profile': src.profile,
                    'block_windows': num_windows,
                    'window_bytes': window_bytes
                }, default=str))

                while range_tracker.try_claim(next_pos):
                    i = math.ceil(next_pos / window_bytes)
                    if i >= num_windows:
                        break

                    cur_window = block_windows[i]
                    cur_transform = src.window_transform(cur_window)
                    block = src.read(self.band_number, window=cur_window)
                    block32 = block.astype(dtype(src_dtype))

                    logging.info(json.dumps({
                        'msg': 'read_records.try_claim',
                        'file_name': file_name,
                        'band': self.band_number,
                        'next_pos': next_pos,
                        'i': i,
                        'window': cur_window
                    }, default=str))

                    for (g, v) in shapes(block32, transform=cur_transform):
                        if self.skip_nodata and v == nodata_val:
                            continue

                        if self.centroid_only:
                            g = shape(g).centroid.__geo_interface__

                        if not self.skip_reproject:
                            g = transform.transform_geom(src_crs, 'epsg:4326', g)

                        yield (v, g)

                    next_pos = next_pos + window_bytes

    def __init__(self, file_pattern, band_number=1, skip_nodata=True,
                 skip_reproject=False, centroid_only=False, in_epsg=None, **kwargs):
        self.band_number = band_number
        self.skip_nodata = skip_nodata
        self.skip_reproject = skip_reproject
        self.centroid_only = centroid_only
        self.in_epsg = in_epsg

        super(GeotiffSource, self).__init__(file_pattern)


class ShapefileSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading shapefiles.

    The given file(s) should be a zip archive containing the .shp file
    alongside the .dbf and .prj files.

    Args:
        layer_name (str, optional): the name of the layer you want to read.
            Required the zipfile contains multiple layers.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.

    Returns:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    Example:
        p | beam.io.Read(ShapefileSource(file_pattern))
    """

    def read_records(self, file_name, range_tracker):
        pass

    def __init__(self, file_pattern, layer_name=None, skip_reproject=False,
                 **kwargs):

        self.layer_name = layer_name
        self.skip_reproject = skip_reproject

        super(ShapefileSource, self).__init__(file_pattern)


class GeodatabaseSource(filebasedsource.FileBasedSource):
    """A Beam FileBasedSource for reading geodatabases.

    The given file(s) should be a zip archive containing .gdb geodatabase
    directory.

    Args:
        layer_name (str): Required. the name of the layer you want to read from
            the gdb.
        skip_reproject (bool, optional): Defaults to `False`. True to return
            `geom` in its original projection.

    Returns:
        generator of (`props`, `geom`) tuples. `props` is a `dict` containing
        all of the feature properties. `geom` is the geometry.

    Example:
        p | beam.io.Read(GeodatabaseSource(file_pattern))
    """

    def read_records(self, file_name, range_tracker):
        pass

    def __init__(self, file_pattern, layer_name=None, skip_reproject=False,
                 **kwargs):

        self.layer_name = layer_name
        self.skip_reproject = skip_reproject

        super(GeodatabaseSource, self).__init__(file_pattern)


class _GeoSourceUtils():
    """Utility methods for the FileBasedSource reader classes"""

    @staticmethod
    def validate_crs(src_crs, in_epsg):
        from fiona import crs

        if in_epsg is not None:
            in_crs = crs.from_epsg(in_epsg)

            if bool(src_crs) is True:
                logging.warning('manually specified CRS {} is being used instead of raster CRS {}'.format(
                    in_crs, src_crs))

            return in_crs

        if bool(src_crs) is False:
                logging.error('--in_epsg must be specified because raster CRS is empty')
                raise Exception()

        return src_crs
