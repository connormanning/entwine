#!/bin/bash

mkdir packages

export CI_PLAT=""
if grep -q "windows" <<< "$PDAL_PLATFORM"; then
    CI_PLAT="win"
    ARCH="64"
fi

if grep -q "ubuntu" <<< "$PDAL_PLATFORM"; then
    CI_PLAT="linux"
    ARCH="64"
fi

if grep -q "macos" <<< "$PDAL_PLATFORM"; then
    CI_PLAT="osx"
    ARCH="arm64"
fi

conda build recipe --clobber-file recipe/recipe_clobber.yaml --output-folder packages -m ".ci_support/${CI_PLAT}_${ARCH}_.yaml"
conda create -y -n test -c ./packages/${CI_PLAT}-${ARCH} entwine
conda deactivate

conda activate test
entwine --version
conda deactivate

