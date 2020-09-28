#!/bin/bash

pwd
where cl.exe
export CC=cl.exe
export CXX=cl.exe
cmake .. -G "Ninja" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" \
    -DWITH_TESTS=ON \
    -DCMAKE_VERBOSE_MAKEFILE=OFF \
    -DCMAKE_LIBRARY_PATH:FILEPATH="$CONDA_PREFIX/Library/lib" \
    -DCMAKE_INCLUDE_PATH:FILEPATH="$CONDA_PREFIX/Library/include" \
    -DBUILD_SHARED_LIBS=ON \
    -Dgtest_force_shared_crt=ON
