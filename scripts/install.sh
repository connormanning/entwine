#!/usr/bin/env bash

./scripts/ubuntu-deps.sh
mkdir build && cd build

cmake -G "Unix Makefiles" \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo ..

make -j4
make install
cd -

