#!/bin/bash -e
# Builds and tests Entwine

clang --version

cd /entwine

mkdir -p _build || exit 1
cd _build || exit 1


cmake \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    ..

# Don't use ninja's default number of threads because it can
# saturate Travis's available memory.
NUMTHREADS=2
make -j ${NUMTHREADS} && \
    LD_LIBRARY_PATH=./lib && \
    ctest -V && \
    make install && \
    /sbin/ldconfig

