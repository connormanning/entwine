#!/bin/bash -e
# Builds and tests Entwine

cd /entwine

mkdir -p build || exit 1
cd build || exit 1

cmake \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=/usr \
    ..

NUMTHREADS=2
make -j ${NUMTHREADS} && \
    make install && \
    ./test/entwine-test

