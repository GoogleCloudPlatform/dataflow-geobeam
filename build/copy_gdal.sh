#!/bin/bash

echo "copying gdal tarball to host..."
docker run -v $PWD:/opt/mount --rm --entrypoint=bash -it gcr.io/geobeam/dataflow-gdal-builder:$1 -c "cp geobeam_gdal-$1.tar.gz /opt/mount"

echo "done."
