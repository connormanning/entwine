#!/bin/bash

echo "Configuring build type '$BUILD_TYPE'"
mkdir build

mamba update -n base -c defaults conda
mamba install cmake ninja compilers -y

mamba install entwine --only-deps -y

gdalinfo --version

