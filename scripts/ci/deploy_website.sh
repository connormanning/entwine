#!/bin/bash

builddir=$1


echo "deploying docs for $TRAVIS_BUILD_DIR/docs"

export AWS_ACCESS_KEY_ID="$AWS_KEY"
export AWS_SECRET_ACCESS_KEY="$AWS_SECRET"

docker run -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET" -e "AWS_ACCESS_KEY_ID=$AWS_KEY" -v $TRAVIS_BUILD_DIR:/data -w /data/doc hobu/entwine-docs aws s3 sync ./build/html/ s3://entwine.io --acl public-read

