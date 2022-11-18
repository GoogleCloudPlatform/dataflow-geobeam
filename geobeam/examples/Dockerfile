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

### Changelog ###
#10.2022 - Dockerfile versions bump to core beam sdk 3.9:2.41 and latest versions of libs (to be ran in GCP Console with Dataflow runner)
#Use with cloudbuild like:
#gcloud builds submit --tag gcr.io/[project]]/geobeam-example --timeout 3600s --machine-type n2-highcpu-16

FROM gcr.io/vadimzaripov-477-2022062208552/geobeam-base

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

RUN python setup.py bdist_wheel
RUN export VERSION=$(python -c 'import geobeam; print(geobeam.__version__)') \
  && pip install dist/geobeam-$VERSION-py3-none-any.whl
