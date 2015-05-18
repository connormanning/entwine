#!/usr/bin/env bash

apt-get update
apt-get install -q -y python-software-properties

# Add PPAs
add-apt-repository -y ppa:ubuntugis/ubuntugis-unstable
add-apt-repository -y ppa:boost-latest/ppa

apt-get update

# Install apt-gettable packages
pkg=(
    git
    ruby
    build-essential
    libjsoncpp-dev
    pkg-config
    cmake
    libgdal-dev
    libpq-dev
    libproj-dev
    libtiff4-dev
    haproxy
    libgeos-dev
    python-all-dev
    python-numpy
    libxml2-dev
    libboost-all-dev
    libbz2-dev
    libsqlite0-dev
    cmake-curses-gui
    postgis
    libcunit1-dev
    libgeos++-dev
)

apt-get install -q -y -V ${pkg[@]}

# Install laz-perf.
git clone https://github.com/verma/laz-perf.git laz-perf
cd laz-perf
git checkout c3d9de3b148a2f4a1ea0844021040addcfccb24f
cmake -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX=/usr .
make
make install
cd -

# Install PDAL.
NUMTHREADS=2
if [[ -f /sys/devices/system/cpu/online ]]; then
	# Calculates 1.5 times physical threads
	NUMTHREADS=$(( ( $(cut -f 2 -d '-' /sys/devices/system/cpu/online) + 1 ) * 15 / 10  ))
fi
export NUMTHREADS
git clone https://github.com/PDAL/PDAL.git pdal
cd pdal
git checkout 272c5a82a79f1073c8cd625c24bb1a27d57c8105
cmake   -G "Unix Makefiles" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DWITH_LASZIP=ON \
        -DWITH_LAZPERF=ON \
        -DWITH_TESTS=OFF
make -j $NUMTHREADS
make install
cd -

