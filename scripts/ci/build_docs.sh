#!/bin/bash

echo "building docs for $TRAVIS_BUILD_DIR/docs"
buildpath=`pwd`
if [[ ! -z $TRAVIS_BUILD_DIR ]]; then
buildpath="$TRAVIS_BUILD_DIR"
fi

docker run -v $buildpath:/data -w /data/docs hobu/entwine-docs make html


