#!/bin/bash

pdoc --pdf \
  --config show_source_code=False \
  geobeam > docs.md

sed -i '/-----=/ s//-----=\n\n/g' docs.md

pandoc \
  --metadata=title:"geobeam Documentation" --from=markdown+abbreviations+tex_math_single_backslash \
  --pdf-engine=xelatex \
  --toc \
  --toc-depth=4 \
  --output=docs.pdf \
  docs.md

gsutil cp docs.pdf gs://geobeam/docs/all.pdf

#rm docs.md
rm docs.pdf
