FROM gcr.io/dataflow-geobeam/base

COPY geobeam/examples/requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

RUN python setup.py bdist_wheel
RUN export VERSION=$(python -c 'import geobeam; print(geobeam.__version__)') \
  && pip install dist/geobeam-$VERSION-py3-none-any.whl
