#!/bin/bash

echo "Configuring build type '$BUILD_TYPE'"
mkdir build

conda update -n base -c defaults conda
conda install cmake ninja compilers -y

if [ "$BUILD_TYPE" == "fixed" ]; then

    conda config --set channel_priority strict
    conda install --yes --quiet pdal=2.2.0=h638e970_7 abseil-cpp  -y
    conda install --yes --quiet entwine  --only-deps -y

else

    conda install entwine --only-deps -y

fi

gdal-config --version

