#!/bin/bash

LDFLAGS="$LDFLAGS -Wl,-rpath-link,$CONDA_PREFIX/lib" cmake .. \
      -G Ninja \
      -DCMAKE_LIBRARY_PATH:FILEPATH="$CONDA_PREFIX/lib" \
      -DCMAKE_INCLUDE_PATH:FILEPATH="$CONDA_PREFIX/include" \
      -DCMAKE_INSTALL_PREFIX=${CONDA_PREFIX} \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      ..
