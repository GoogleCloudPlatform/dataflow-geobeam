FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ENV PATH=$PATH:$WORKDIR/build/usr/local/bin
ENV CURL_VERSION 7.73.0
ENV GEOS_VERSION 3.9.0
ENV SQLITE_VERSION 3330000
ENV SQLITE_YEAR 2020
ENV PROJ_VERSION 7.2.0
ENV OPENJPEG_VERSION 2.3.1
ENV GDAL_VERSION 3.2.1

# cram into a single RUN in order to reduce image size by deleting artifacts
# afterward
RUN apt-get update -y \
    && apt-get install libffi-dev git g++ make cmake automake pkg-config -y \
    && wget -q https://curl.haxx.se/download/curl-${CURL_VERSION}.tar.gz \
    && tar -xzf curl-${CURL_VERSION}.tar.gz && cd curl-${CURL_VERSION} \
    && ./configure --prefix=$WORKDIR/build/usr/local \
    && echo "building CURL ${CURL_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
      \
    && wget -q https://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2 \
    && tar -xjf geos-${GEOS_VERSION}.tar.bz2  \
    && cd geos-${GEOS_VERSION} \
    && ./configure --prefix=$WORKDIR/build/usr/local \
    && echo "building geos ${GEOS_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
      \
    && wget -q https://sqlite.org/${SQLITE_YEAR}/sqlite-autoconf-${SQLITE_VERSION}.tar.gz \
    && tar -xzf sqlite-autoconf-${SQLITE_VERSION}.tar.gz && cd sqlite-autoconf-${SQLITE_VERSION} \
    && ./configure --prefix=$WORKDIR/build/usr/local \
    && echo "building SQLITE ${SQLITE_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
      \
    && wget -q https://download.osgeo.org/proj/proj-${PROJ_VERSION}.tar.gz \
    && tar -xzf proj-${PROJ_VERSION}.tar.gz \
    && cd proj-${PROJ_VERSION} \
    && CFLAGS='-DPROJ_RENAME_SYMBOLS -O2' CXXFLAGS='-DPROJ_RENAME_SYMBOLS -DPROJ_INTERNAL_CPP_NAMESPACE -O2' \
        PKG_CONFIG_PATH=$WORKDIR/build/usr/local/lib/pkgconfig  \
        ./configure \
            --prefix=$WORKDIR/build/usr/local \
            --without-curl \
            --disable-static \
    && echo "building proj ${PROJ_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
      \
    && wget -q -O openjpeg-${OPENJPEG_VERSION}.tar.gz https://github.com/uclouvain/openjpeg/archive/v${OPENJPEG_VERSION}.tar.gz \
    && tar -zxf openjpeg-${OPENJPEG_VERSION}.tar.gz \
    && cd openjpeg-${OPENJPEG_VERSION} \
    && mkdir build && cd build \
    && cmake .. -DBUILD_THIRDPARTY:BOOL=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$WORKDIR/build/usr/local \
    && echo "building openjpeg ${OPENJPEG_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
      \
    && wget -q https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && tar -xzf gdal-${GDAL_VERSION}.tar.gz && cd gdal-${GDAL_VERSION} \
    && ./configure \
          --disable-debug \
          --prefix=$WORKDIR/build/usr/local \
          --disable-static \
          --without-curl \
          --without-libtool \
          --with-geos=$WORKDIR/build/usr/local \
          --with-geotiff=internal --with-rename-internal-libgeotiff-symbols \
          --with-hide-internal-symbols \
          --with-libtiff=internal --with-rename-internal-libtiff-symbols \
          --with-openjpeg=$WORKDIR/build/usr/local \
          --with-sqlite3=$WORKDIR/build/usr/local \
          --with-proj=$WORKDIR/build/usr/local \
          --with-rename-internal-libgeotiff-symbols=yes \
          --with-rename-internal-libtiff-symbols=yes \
          --with-threads=yes \
    && echo "building GDAL ${GDAL_VERSION}..." \
    && make --quiet -j$(nproc) && make --quiet install \
      \
    && cd $WORKDIR/build \
    && tar czf geobeam_gdal-$GDAL_VERSION.tar.gz * \
    && mv geobeam_gdal-$GDAL_VERSION.tar.gz .. \
      \
    && cd $WORKDIR && rm -rf curl* proj* gdal* geos* openjpeg* sqlite* build \
    && apt-get clean

ENTRYPOINT "bash"
