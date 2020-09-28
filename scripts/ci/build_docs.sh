#!/bin/bash

echo "building docs for $TRAVIS_BUILD_DIR/doc"
buildpath=`pwd`
if [[ ! -z $TRAVIS_BUILD_DIR ]]; then
buildpath="$TRAVIS_BUILD_DIR"
fi

# osgeo/proj-docs contains everything to build the website and
# it is kept up to date by the PROJ team
docker run -v $buildpath:/data -w /data/doc osgeo/proj-docs make html
docker run -v $buildpath:/data -w /data/doc osgeo/proj-docs make latexpdf
docker run -v $buildpath:/data -w /data/doc osgeo/proj-docs make spelling


