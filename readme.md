![Entwine logo](./doc/logo/color/entwine_logo_2-color-small.png)


## Build Status

[![OSX](https://github.com/connormanning/entwine/workflows/OSX/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3AOSX)
[![Linux](https://github.com/connormanning/entwine/workflows/Linux/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3ALinux)
[![Windows](https://github.com/connormanning/entwine/workflows/Windows/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3AWindows)
[![Docs](https://github.com/connormanning/entwine/workflows/Docs/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3ADocs)
[![Conda](https://github.com/connormanning/entwine/workflows/Conda/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3AConda)
[![Docs](https://github.com/connormanning/entwine/workflows/Docs/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3ADocs)
[![Docker](https://github.com/connormanning/entwine/workflows/Docker/badge.svg)](https://github.com/connormanning/entwine/actions?query=workflow%3ADocker)

Entwine is a data organization library for massive point clouds, designed to conquer datasets of hundreds of billions of points as well as desktop-scale point clouds.  Entwine can index anything that is [PDAL](https://pdal.io)-readable, and can read/write to a variety of sources like S3 or Dropbox.  Builds are completely lossless, so no points will be discarded even for terabyte-scale datasets.

Check out the client demos, showcasing Entwine output with [Cesium](https://viewer.copc.io?state=209b4b8dc400dd769eed0b8c15ecb46de666b10658fb12fc9c32c81f48242ad1) (see
"Sample Data" section) and [Potree](http://potree.entwine.io) clients.

Usage
--------------------------------------------------------------------------------

Getting started with Entwine is easy with [Conda](https://conda.io/docs/).  First,
create an environment with the `entwine` package, then activate this environment:
```
conda create --yes --name entwine --channel conda-forge entwine
conda activate entwine
```

Now we can index some public data:
```
entwine build \
    -i https://data.entwine.io/red-rocks.laz \
    -o ~/entwine/red-rocks
```

Now we have our output at `~/entwine/red-rocks`.  We could have also passed a directory like `-i ~/county-data/` to index multiple files.  Now we can
statically serve `~/entwine` with a simple HTTP server:
```
docker run -it -v ~/entwine:/var/www -p 8080:8080 connormanning/http-server
```

And view the data with [Cesium](https://viewer.copc.io/?q=http://localhost:8080/red-rocks/ept.json) or [Potree](http://potree.entwine.io/data/custom.html?r=http://localhost:8080/red-rocks/ept.json).

Going further
--------------------------------------------------------------------------------

For detailed information about how to configure your builds, check out the [configuration documentation](https://entwine.io/configuration.html).  Here, you can find information about reprojecting your data, using configuration files and templates, enabling S3 capabilities, and all sorts of other settings.

To learn about the Entwine Point Tile (EPT) file format produced by Entwine, see the [file format documentation](https://entwine.io/entwine-point-tile.html).

For an alternative method of generating EPT which can also generate [COPC](https://copc.io) data, see the [Untwine](https://github.com/hobuinc/untwine) project.
