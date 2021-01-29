FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

RUN apt update && apt install pandoc texlive-xetex -y
RUN python3 -m pip install pdoc3 pandoc

COPY . .

RUN python3 -m pip install -r docs/requirements.txt

RUN bash docs/make_docs.sh

