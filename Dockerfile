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

FROM apache/beam_python3.8_sdk:2.41.0

ARG WORKDIR=/pipeline
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ENV CCACHE_DISABLE=1
ENV PATH=$PATH:$WORKDIR/build/usr/local/bin

RUN apt-get update -y \
    && apt-get install libffi-dev g++ cmake automake pkg-config -y \
    && apt-get clean

ENV CURL_VERSION 7.73.0
RUN wget -q https://curl.haxx.se/download/curl-${CURL_VERSION}.tar.gz \
    && tar -xzf curl-${CURL_VERSION}.tar.gz && cd curl-${CURL_VERSION} \
    && ./configure --prefix=/usr/local \
    && echo "building CURL ${CURL_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf curl-${CURL_VERSION}.tar.gz curl-${CURL_VERSION}

ENV GEOS_VERSION 3.9.0
RUN wget -q https://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2 \
    && tar -xjf geos-${GEOS_VERSION}.tar.bz2  \
    && cd geos-${GEOS_VERSION} \
    && ./configure --prefix=/usr/local \
    && echo "building geos ${GEOS_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf geos-${GEOS_VERSION}.tar.bz2 geos-${GEOS_VERSION}

ENV SQLITE_VERSION 3330000
ENV SQLITE_YEAR 2020
RUN wget -q https://sqlite.org/${SQLITE_YEAR}/sqlite-autoconf-${SQLITE_VERSION}.tar.gz \
    && tar -xzf sqlite-autoconf-${SQLITE_VERSION}.tar.gz && cd sqlite-autoconf-${SQLITE_VERSION} \
    && ./configure --prefix=/usr/local \
    && echo "building SQLITE ${SQLITE_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf sqlite-autoconf-${SQLITE_VERSION}.tar.gz sqlite-autoconf-${SQLITE_VERSION}

ENV PROJ_VERSION 9.1.0
RUN wget -q https://download.osgeo.org/proj/proj-${PROJ_VERSION}.tar.gz \
    && tar -xzf proj-${PROJ_VERSION}.tar.gz \
    && cd proj-${PROJ_VERSION} && mkdir build && cd build \
    && CFLAGS='-DPROJ_RENAME_SYMBOLS -O2' CXXFLAGS='-DPROJ_RENAME_SYMBOLS -DPROJ_INTERNAL_CPP_NAMESPACE -O2' PKG_CONFIG_PATH=/usr/local/lib/pkgconfig  \
    && echo "building proj ${PROJ_VERSION}..." \
    && cmake .. \
    && cmake --build . \
    && cmake --build . --target install \
    && cd $WORKDIR && rm -rf proj-${PROJ_VERSION}.tar.gz proj-${PROJ_VERSION}

ENV OPENJPEG_VERSION 2.3.1
RUN wget -q -O openjpeg-${OPENJPEG_VERSION}.tar.gz https://github.com/uclouvain/openjpeg/archive/v${OPENJPEG_VERSION}.tar.gz \
    && tar -zxf openjpeg-${OPENJPEG_VERSION}.tar.gz \
    && cd openjpeg-${OPENJPEG_VERSION} \
    && mkdir build && cd build \
    && cmake .. -DBUILD_THIRDPARTY:BOOL=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
    && echo "building openjpeg ${OPENJPEG_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
    && cd $WORKDIR && rm -rf openjpeg-${OPENJPEG_VERSION}.tar.gz openjpeg-${OPENJPEG_VERSION}

ENV GDAL_VERSION 3.5.2
RUN wget -q https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && tar -xzf gdal-${GDAL_VERSION}.tar.gz && cd gdal-${GDAL_VERSION} && mkdir build && cd build \
    && echo "building GDAL ${GDAL_VERSION}..." \
    && cmake .. \
    && cmake --build . \
    && cmake --build . --target install \
    && cd $WORKDIR && rm -rf gdal-${GDAL_VERSION}.tar.gz gdal-${GDAL_VERSION}

RUN apt-get remove g++ cmake automake pkg-config -y \
  && apt-get clean

RUN cd $WORKDIR
RUN ldconfig
RUN pip install --upgrade pip
COPY . .
RUN pip install .
