#!/bin/bash

# pdal/doc image has all of the Sphinx
# dependencies need to build PDAL's docs

docker pull connormanning/pdal:entwine-pin
docker pull hobu/entwine-docs

