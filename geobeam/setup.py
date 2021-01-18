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

import setuptools
import logging

GDAL_VERSION = '3.2.1'
DIST_URL = 'https://storage.googleapis.com/geobeam/dist/geobeam_gdal_{}.tar.gz'.format(GDAL_VERSION)


class GeobeamCommands(setuptools.Command):
    """A setuptools Command class that installs geobeam dependencies"""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def gdal_install(self):
        import urllib.request
        import tarfile
        import subprocess

        tar_filename = 'gdal-{}.tar.gz'.format(GDAL_VERSION)
        urllib.request.urlretrieve(DIST_URL, tar_filename)
        with tarfile.open(tar_filename) as tar:
            tar.extractall(path='/')

        subprocess.check_output(['ldconfig'], universal_newlines=True).strip()

    def has_gdal(self):
        import subprocess
        try:
            found_version = subprocess.check_output(['gdal-config', '--version'], universal_newlines=True).strip()
            if found_version[0] != GDAL_VERSION[0]:
                logging.error('gdal {} found, but is not compatible with gdal {}, which is required by geobeam'.format(
                    found_version, GDAL_VERSION))

            if found_version != GDAL_VERSION:
                logging.warning('gdal {} found, but geobeam wants {}. attempting to proceed anyway'.format(
                    found_version, GDAL_VERSION))

            return True

        except FileNotFoundError:
            return False

    def pip_install(self):
        import pip

        try:
            pip.main(['install', 'gdal==3.2.1'])
            pip.main(['install', 'pyproj==3.0.0.post1'])
            pip.main(['install', 'fiona==1.8.18'])
            pip.main(['install', 'shapely==1.7.1'])
            pip.main(['install', 'rasterio==1.1.8'])
        except SystemExit as e:
            logging.error(e)

    def run(self):
        if not self.has_gdal():
            self.gdal_install()

        self.pip_install()
