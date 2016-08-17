#!/bin/bash

echo "building docs for $TRAVIS_BUILD_DIR/doc"
buildpath=`pwd`
if [[ ! -z $TRAVIS_BUILD_DIR ]]; then
buildpath="$TRAVIS_BUILD_DIR"
fi

docker run -v $buildpath:/data -w /data/doc hobu/entwine-docs make html


