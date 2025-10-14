#!/bin/bash

pwd
ls
git clone https://github.com/conda-forge/entwine-feedstock.git

cd entwine-feedstock

yq -y -i '.source.url = ""' recipe/recipe.yaml
yq -y -i '.source.sha256 = ""' recipe/recipe.yaml
yq -y -i '.source.path = "../../"' recipe/recipe.yaml
yq -y -i '.build.number = 2112' recipe/recipe.yaml


ls recipe
