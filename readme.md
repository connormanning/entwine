![Entwine logo](./doc/logo/color/entwine_logo_2-color-small.png)

[![Build Status](https://dev.azure.com/hobuinc/Entwine/_apis/build/status/connormanning.entwine?branchName=master)](https://dev.azure.com/hobuinc/Entwine/_build/latest?definitionId=1&branchName=master)


Entwine is a data organization library for massive point clouds, designed to conquer datasets of hundreds of billions of points as well as desktop-scale point clouds.  Entwine can index anything that is [PDAL](https://pdal.io)-readable, and can read/write to a variety of sources like S3 or Dropbox.  Builds are completely lossless, so no points will be discarded even for terabyte-scale datasets.

Check out the client demos, showcasing Entwine output with [Potree](http://potree.entwine.io), [Plas.io](http://speck.ly), and [Cesium](http://cesium.entwine.io) clients.

Usage
--------------------------------------------------------------------------------

Getting started with Entwine is easy with [Docker](http://docker.com).  First, we can index some public data:

```
mkdir ~/entwine
docker run -it -v ~/entwine:/entwine connormanning/entwine build \
    -i https://data.entwine.io/red-rocks.laz \
    -o /entwine/red-rocks
```

Now we have our output at `~/entwine/red-rocks`.  We could have also passed a directory like `-i ~/county-data/` to index multiple files.  Now we can
statically serve `~/entwine` with a simple HTTP server:

```
docker run -it -v ~/entwine:/var/www -p 8080:8080 connormanning/http-server
```

And view the data with [Potree](http://potree.entwine.io/data/custom.html?r=http://localhost:8080/red-rocks/ept.json) and [Plasio](http://dev.speck.ly/?s=0&r=ept://localhost:8080/red-rocks&c0s=local://color).

Going further
--------------------------------------------------------------------------------

For detailed information about how to configure your builds, check out the [configuration documentation](https://entwine.io/configuration.html).  Here, you can find information about reprojecting your data, using configuration files and templates, enabling S3 capabilities, producing [Cesium 3D Tiles](https://github.com/AnalyticalGraphicsInc/3d-tiles) output, and all sorts of other settings.

To learn about the Entwine Point Tile file format produced by Entwine, see the [file format documentation](https://entwine.io/entwine-point-tile.html).

