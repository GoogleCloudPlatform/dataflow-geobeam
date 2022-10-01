import os
import math
import logging
import time
import rasterio
from rasterio.io import MemoryFile
from rasterio.features import shapes
from rasterio.windows import union
from rasterio.warp import transform_geom
from shapely.geometry import shape
import json

def read_records(file_name, src_crs, centroid_only):
    #total_bytes = os.path.getsize(file_name)

    # do first window
    t0 = time.process_time()
    with open(file_name, 'rb') as f, MemoryFile(f.read()) as m, m.open() as src:
    #with open(file_name, 'rb') as f, rasterio.open(f) as src:
        logging.info('opening file time: {}'.format(time.process_time() - t0))

        t1 = time.process_time()
        block_windows = list([win for ji, win in src.block_windows()])
        logging.info('list block windows: {}'.format(time.process_time() - t1))
        num_windows = len(block_windows)
        #window_bytes = math.floor(total_bytes / num_windows)
        t2 = time.process_time()
        window_slice = block_windows[0:math.ceil(num_windows / 2)]
        cur_window = union(window_slice)
        cur_transform = src.window_transform(cur_window)
        block_mask = src.read_masks(1, window=cur_window)
        block = src.read(1, window=cur_window, masked=True)
        logging.info('windowing stuff + src.read {}'.format(time.process_time() - t2))

        t3 = time.process_time()
        block_shapes = shapes(block, block_mask, transform=cur_transform)
        logging.info('shapes {}'.format(time.process_time() - t3))

        for (g, v) in block_shapes:
            #geom = transform_geom(src_crs, 'epsg:4326', g)

            if centroid_only:
                geom = shape(g).centroid.__geo_interface__

            #logging.info('{} {}'.format(geom, v))

        logging.info('reading all geoms {}'.format(time.process_time() - t3))
            #yield (v, geom)

if __name__ == '__main__':
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_name', type=str)
    parser.add_argument('--src_crs', type=str, default='epsg:4326')
    parser.add_argument('--centroid_only', type=bool, default=False)
    args, _ = parser.parse_known_args()

    read_records(args.file_name, args.src_crs, args.centroid_only)
