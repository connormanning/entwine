#!/usr/bin/env bash

./entwine/scripts/ubuntu-deps.sh
mkdir -p ./entwine/build && cd ./entwine/build

cmake -G "Unix Makefiles" \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo ..

make -j4
make install
cd -

