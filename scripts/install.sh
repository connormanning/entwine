#!/usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/..

./scripts/ubuntu-deps.sh
mkdir -p build && cd build

cmake -G "Unix Makefiles" \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DCMAKE_BUILD_TYPE=Release ..

make -j4
make install
cd -

