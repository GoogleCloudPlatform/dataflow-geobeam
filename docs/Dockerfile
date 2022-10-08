FROM python:3.8-buster

RUN apt update
RUN apt install latexmk texlive-latex-recommended texlive-pictures texlive-latex-extra -y
RUN pip install --upgrade pip
RUN python -m pip install sphinx m2r2 apache_beam jinja2==3.0.3
