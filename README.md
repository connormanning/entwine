![Entwine logo](./doc/logo/color/entwine_logo_2-color-small.png)

[![Build Status](https://travis-ci.org/connormanning/entwine.svg?branch=master)](https://travis-ci.org/connormanning/entwine)



Entwine is a data organization library for massive point clouds, designed to conquer datasets of hundreds of billions of points as well as desktop-scale point clouds.  Entwine can index anything that is [PDAL](https://pdal.io)-readable, and can read/write to a variety of sources like S3 or Dropbox.  Builds are completely lossless, so no points will be discarded even for terabyte-scale datasets.

Check out the client demos, showcasing Entwine output with [Plas.io](http://speck.ly>), [Potree](http://potree.entwine.io), and [Cesium](http://cesium.entwine.io) clients.

Usage
--------------------------------------------------------------------------------

Getting started with Entwine is easy with [Docker](http://docker.com).  Pull the most recent image with `docker pull connormanning/entwine`.  Let's build an Entwine index of some publicly hosted data:

```
docker run -it -v $HOME:$HOME connormanning/entwine build \
    -i https://entwine.io/sample-data/red-rocks.laz \
    -o ~/entwine/red-rocks
```

Now we have our output at `~/entwine/red-rocks`.  We could have also passed a directory like `-i ~/county-data/` to index multiple files.  Now we can view this data with [Greyhound](https://github.com/hobu/greyhound) - we'll map our top-level Entwine output directory into one of the default search paths for the Greyhound container.

```
docker run -it -v ~/entwine:/entwine -p 8080:8080 connormanning/greyhound
```

Now that we have Greyhound running locally and ready to serve our data, we can view it with these [Plasio](http://speck.ly/?s=http://localhost:8080/&r=red-rocks) or [Potree](http://potree.entwine.io/data/custom.html?s=localhost:8080&r=red-rocks) links which point at our local resource.

Going further
--------------------------------------------------------------------------------

For detailed information about how to configure your builds, check out the [configuration documentation](doc/source/configuration.rst).  Here, you can find information about reprojecting your data, using configuration files and templates, enabling S3 capabilities, producing [Cesium 3D Tiles](https://github.com/AnalyticalGraphicsInc/3d-tiles) output, and all sorts of other settings.
