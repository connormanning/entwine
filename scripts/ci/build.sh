#!/bin/bash -e
# Builds and tests Entwine

cd /entwine

mkdir -p _build || exit 1
cd _build || exit 1

cmake \
    -g "Unix Makefiles" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=/usr \
    ..

# Don't use ninja's default number of threads because it can
# saturate Travis's available memory.
NUMTHREADS=2
make -j ${NUMTHREADS} && \
    make install && \
    (cd /entwine/test/data && ./generate) &&
    ./test/entwine-test

