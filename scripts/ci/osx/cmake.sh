#!/bin/bash

cmake .. \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_INSTALL_PREFIX=`pwd`/../install \
      -DWITH_ZSTD=ON \
      -DWITH_ZLIB=ON \
      -DWITH_TESTS=ON
