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

from __future__ import absolute_import

import setuptools
from distutils.command.build import build as _build
from distutils.core import setup
import geobeam

REQUIRED_PACKAGES = [
    'apache_beam[gcp]>=2.27.0',
    'pyproj==3.0.0.post1',
    'fiona==1.8.18',
    'shapely==1.7.1',
    'rasterio==1.1.8'
]


class build(_build):
    sub_commands = _build.sub_commands + [('GeobeamCommands', None)]


with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='geobeam',
    version=geobeam.__version__,
    author='Travis Webb',
    author_email='traviswebb@google.com',
    description='geobeam adds GIS capabilities to your Apache Beam pipelines',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords='beam dataflow gdal gis',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: GIS',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers'
    ],
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    python_requires='>=3.7'
)
