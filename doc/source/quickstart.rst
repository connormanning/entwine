.. _quickstart:

******************************************************************************
Quickstart
******************************************************************************

Getting started with Entwine is easy with `Docker`_. First, we can index
some public data:

::

   mkdir ~/entwine
   docker run -it -v ~/entwine:/entwine connormanning/entwine build \
       -i https://entwine.io/data/red-rocks.laz \
       -o /entwine/red-rocks

Now we have our output at ``~/entwine/red-rocks``. We could have also
passed a directory like ``-i ~/county-data/`` to index multiple files.
Now we can statically serve ``~/entwine`` with a simple HTTP server:

::

   docker run -it -v ~/entwine:/var/www -p 8080:8080 connormanning/http-server

And view the data with `Potree`_ and `Plasio`_.

Going further
-------------

For detailed information about how to configure your builds, check out
the `configuration documentation`_. Here, you can find information about
reprojecting your data, using configuration files and templates,
enabling S3 capabilities, producing `Cesium 3D Tiles`_ output, and all
sorts of other settings.

To learn about the Entwine Point Tile file format produced by Entwine,
see the `file format documentation`_.

.. _Docker: http://docker.com
.. _Potree: http://potree.entwine.io/data/custom.html?r=http://localhost:8080/red-rocks/ept.json
.. _Plasio: http://dev.speck.ly/?s=0&r=ept://localhost:8080/red-rocks&c0s=local://color
.. _Cesium 3D Tiles: https://github.com/AnalyticalGraphicsInc/3d-tiles
.. _configuration documentation: https://github.com/connormanning/entwine/blob/master/doc/configuration.md
.. _file format documentation: https://github.com/connormanning/entwine/blob/master/doc/entwine-point-tile.md

