FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
ARG py_file
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

RUN apt-get update -y
RUN apt-get install libffi-dev git -y
RUN pip install --upgrade pip

COPY examples/requirements.txt ${WORKDIR}/requirements.txt

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

RUN pip install -r requirements.txt

COPY setup.py ${WORKDIR}/setup.py
COPY $py_file ${WORKDIR}/main.py
COPY . .
