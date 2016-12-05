#!/bin/bash

# pdal/doc image has all of the Sphinx
# dependencies need to build PDAL's docs

docker pull pdal/pdal:latest
docker pull hobu/entwine-docs

