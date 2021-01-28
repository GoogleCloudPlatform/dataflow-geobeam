FROM gcr.io/dataflow-geobeam/base:3.2.1

COPY examples/requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN mkdir -p examples geobeam dist

COPY . .

RUN python setup.py bdist_wheel

RUN pip install dist/geobeam-0.0.1-py3-none-any.whl
