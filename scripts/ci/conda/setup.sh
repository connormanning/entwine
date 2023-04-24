#!/bin/bash

mamba update -n base -c defaults conda -y
mamba install conda-build ninja compilers -y
pwd
ls
git clone https://github.com/conda-forge/entwine-feedstock.git

cd entwine-feedstock
cat > recipe/recipe_clobber.yaml <<EOL
source:
  path: ../../
  url:
  sha256:

build:
  number: 2112
EOL

ls recipe
