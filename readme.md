![Entwine logo](./doc/logo/color/entwine_logo_2-color-small.png)

[![Build Status](https://travis-ci.org/connormanning/entwine.svg?branch=master)](https://travis-ci.org/connormanning/entwine)



Entwine is a data organization library for massive point clouds, designed to conquer datasets of hundreds of billions of points as well as desktop-scale point clouds.  Entwine can index anything that is [PDAL](https://pdal.io)-readable, and can read/write to a variety of sources like S3 or Dropbox.  Builds are completely lossless, so no points will be discarded even for terabyte-scale datasets.

Check out the client demos, showcasing Entwine output with [Potree](http://potree.entwine.io), [Plas.io](http://speck.ly), and [Cesium](http://cesium.entwine.io) clients.

Usage
--------------------------------------------------------------------------------

Getting started with Entwine is easy with [Docker](http://docker.com).  Pull the most recent image with `docker pull connormanning/entwine:ept`, and index some public data:

```
cd ~
mkdir entwine
docker run -it -v entwine:/entwine connormanning/entwine:ept build \
    -i https://entwine.io/sample-data/red-rocks.laz \
    -o /entwine/red-rocks
```

Now we have our output at `~/entwine/red-rocks`.  We could have also passed a directory like `-i ~/county-data/` to index multiple files.  Now we can
statically serve `~/entwine` with an HTTP server:

```
docker run -it -v entwine:/var/www -p 8080:8080 connormanning/http-server
```

And view the data with [Potree](http://potree.entwine.io/data/custom.html?r="http://localhost:8080/red-rocks/entwine.json").

Going further
--------------------------------------------------------------------------------

For detailed information about how to configure your builds, check out the [configuration documentation](doc/configuration.md).  Here, you can find information about reprojecting your data, using configuration files and templates, enabling S3 capabilities, producing [Cesium 3D Tiles](https://github.com/AnalyticalGraphicsInc/3d-tiles) output, and all sorts of other settings.

To learn about the Entwine Point Tile file format produced by Entwine, see the [file format documentation](doc/entwine-point-tile.md).

