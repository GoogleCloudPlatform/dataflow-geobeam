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

FROM apache/beam_python3.8_sdk:2.40.0

ARG WORKDIR=/pipeline
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ENV CCACHE_DISABLE=1
ENV PATH=$PATH:$WORKDIR/build/usr/local/bin
ENV CLOUDSDK_PYTHON_SITEPACKAGES=1

RUN apt-get update -y \
    && apt-get install libffi-dev git g++ gfortran make cmake automake \
    pkg-config flex bison libeccodes-dev libeccodes-tools metview=5.10.2-1 \
    metview-data=5.10.2-1 libmetview0d=5.10.2-1 libmetview-dev=5.10.2-1 -y \
    && apt-get clean

# Fixes the metview issue with debian
RUN mkdir -p /usr/share/metview/share
RUN ln -s /usr/share/metview/*  /usr/share/metview/share/
RUN ln -s /usr/lib/ /usr/share/metview/

ENV CURL_VERSION 7.83.1
RUN wget -q https://curl.haxx.se/download/curl-${CURL_VERSION}.tar.gz \
    && tar -xzf curl-${CURL_VERSION}.tar.gz && cd curl-${CURL_VERSION} \
    && ./configure --prefix=/usr/local \
    --with-openssl \
    && echo "building CURL ${CURL_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf curl-${CURL_VERSION}.tar.gz curl-${CURL_VERSION}

ENV GEOS_VERSION 3.10.3
RUN wget -q https://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2 \
    && tar -xjf geos-${GEOS_VERSION}.tar.bz2  \
    && cd geos-${GEOS_VERSION} \
    && ./configure --prefix=/usr/local \
    && echo "building geos ${GEOS_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf geos-${GEOS_VERSION}.tar.bz2 geos-${GEOS_VERSION}

ENV SQLITE_VERSION 3380500
ENV SQLITE_YEAR 2022
RUN wget -q https://sqlite.org/${SQLITE_YEAR}/sqlite-autoconf-${SQLITE_VERSION}.tar.gz \
    && tar -xzf sqlite-autoconf-${SQLITE_VERSION}.tar.gz && cd sqlite-autoconf-${SQLITE_VERSION} \
    && ./configure --prefix=/usr/local \
    && echo "building SQLITE ${SQLITE_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf sqlite-autoconf-${SQLITE_VERSION}.tar.gz sqlite-autoconf-${SQLITE_VERSION}

ENV PROJ_VERSION 9.0.0
RUN wget -q https://download.osgeo.org/proj/proj-${PROJ_VERSION}.tar.gz \
    && tar -xzf proj-${PROJ_VERSION}.tar.gz \
    && cd proj-${PROJ_VERSION} \
    && mkdir build && cd build \
    && PKG_CONFIG_PATH=/usr/local/lib/pkgconfig cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DENABLE_CURL=OFF -DBUILD_PROJSYNC=OFF \
    && echo "building proj ${PROJ_VERSION}..." \
    && cmake --build . && cmake --build . --target install \
    && cd $WORKDIR && rm -rf proj-${PROJ_VERSION}.tar.gz proj-${PROJ_VERSION}

ENV OPENJPEG_VERSION 2.5.0
RUN wget -q -O openjpeg-${OPENJPEG_VERSION}.tar.gz https://github.com/uclouvain/openjpeg/archive/v${OPENJPEG_VERSION}.tar.gz \
    && tar -zxf openjpeg-${OPENJPEG_VERSION}.tar.gz \
    && cd openjpeg-${OPENJPEG_VERSION} \
    && mkdir build && cd build \
    && cmake .. -DBUILD_THIRDPARTY:BOOL=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
    && echo "building openjpeg ${OPENJPEG_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf openjpeg-${OPENJPEG_VERSION}.tar.gz openjpeg-${OPENJPEG_VERSION}

ENV HDF5_VERSION 1.10.5
RUN wget -q https://support.hdfgroup.org/ftp/HDF5/current/src/hdf5-${HDF5_VERSION}.tar.gz \
    && tar -xzf hdf5-${HDF5_VERSION}.tar.gz \
    && cd hdf5-${HDF5_VERSION} \
    && mkdir install \
    && ./configure --prefix=/usr/local \
    && make install \
    && cd $WORKDIR && rm -rf hdf5-${HDF5_VERSION}.tar.gz hdf5-${HDF5_VERSION}

ENV NETCDF_VERSION 4.9.0
RUN wget -q https://downloads.unidata.ucar.edu/netcdf-c/${NETCDF_VERSION}/netcdf-c-${NETCDF_VERSION}.tar.gz \
    && tar -xzf netcdf-c-${NETCDF_VERSION}.tar.gz \
    && cd netcdf-c-${NETCDF_VERSION} \
    && mkdir install \
    && ./configure --prefix=/usr/local -disable-dap \
    && make install \
    && cd $WORKDIR && rm -rf netcdf-c-${NETCDF_VERSION}.tar.gz netcdf-c-${NETCDF_VERSION}

ENV GDAL_VERSION 3.5.1
RUN wget -q https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && tar -xzf gdal-${GDAL_VERSION}.tar.gz && cd gdal-${GDAL_VERSION} \
    && mkdir build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DGDAL_USE_CURL=ON \
    -DGDAL_USE_SQLITE3=ON \
    -DGDAL_USE_GEOS=ON \
    -DGDAL_USE_TIFF_INTERNAL=ON \
    -DGDAL_USE_GEOTIFF_INTERNAL=ON \
    -DGEOS_INCLUDE_DIR=/usr/local/include/ \
    -DGEOS_LIBRARY=/usr/local/lib/ \
    && echo "building GDAL ${GDAL_VERSION}..." \
    && cmake --build . && cmake --build . --target install \
    && cd $WORKDIR && rm -rf gdal-${GDAL_VERSION}.tar.gz gdal-${GDAL_VERSION}

RUN ldconfig
RUN pip install --upgrade pip
RUN pip install gdal[numpy]==$GDAL_VERSION

#https://cloud.google.com/iap/docs/using-tcp-forwarding#increasing_the_tcp_upload_bandwidth
RUN $(gcloud info --format="value(basic.python_location)") -m pip install numpy
RUN gcloud components install alpha