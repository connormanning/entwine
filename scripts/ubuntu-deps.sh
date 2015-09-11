#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../..

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

# Install node.
nodeVersion="0.12.7"
nodeUrl="http://nodejs.org/dist/v$nodeVersion/node-v$nodeVersion-linux-x64.tar.gz"
echo Provisioning node.js version $nodeVersion...
mkdir -p /tmp/nodejs
wget -qO - $nodeUrl | tar zxf - --strip-components 1 -C /tmp/nodejs
cd /tmp/nodejs
cp -r * /usr
cd -

npm install -g nodeunit node-gyp
npm update npm -g
npm cache clean

# Install las-zip.
git clone https://github.com/LASzip/LASzip.git laszip
cd laszip
cmake . -DCMAKE_INSTALL_PREFIX=/usr && make && make install
cd -

# Install laz-perf.
git clone https://github.com/verma/laz-perf.git laz-perf
cd laz-perf
git checkout c3d9de3b148a2f4a1ea0844021040addcfccb24f
cmake -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX=/usr . && make && make install
cd -

# Install PDAL.
NUMTHREADS=2
if [[ -f /sys/devices/system/cpu/online ]]; then
	# Calculates 1.5 times physical threads
	NUMTHREADS=$((($(cut -f 2 -d '-' /sys/devices/system/cpu/online) + 1) * 15 / 10))
fi
export NUMTHREADS
git clone https://github.com/PDAL/PDAL.git pdal
cd pdal
git checkout 20b7dfaa7082749edc2a33b6825c2ba15dc42335
cmake   -G "Unix Makefiles" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DWITH_LASZIP=ON \
        -DWITH_LAZPERF=ON \
        -DWITH_TESTS=OFF
make -j $NUMTHREADS && make install
cd -

