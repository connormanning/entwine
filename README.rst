.. image:: ./doc/logo/color/entwine_logo_2-color-small.png

|
|

Entwine is a data organization library for massive point clouds, designed to conquer datasets of hundreds of billions of points as well as desktop-scale point clouds.  Entwine can index anything that is `PDAL`_-readable, and can read/write to a variety of sources like S3 or Dropbox.  Builds are completely lossless, so no points will be discarded even for terabyte-scale datasets.

Check out the client demos, showcasing Entwine output with `Plas.io <http://speck.ly>`_, `Potree <http://potree.entwine.io>`_, and `Cesium <http://cesium.entwine.io>`_ clients.

Usage
--------------------------------------------------------------------------------

Getting started with Entwine is easy with `Docker`_.  Pull the most recent image with ``docker pull connormanning/entwine``.  Let's build an Entwine index of some publicly hosted data:

::

    docker run -it -v $HOME:/opt/data connormanning/entwine \
        entwine build \
            -i https://entwine.io/sample-data/red-rocks.laz \
            -o /opt/data/entwine/red-rocks

|

Now we have our Entwine data at ``~/entwine/red-rocks``.  We could have also passed a directory like ``-i /opt/data/county-data/`` to index multiple files.  Now we can view this data with `Greyhound`_.

::

    docker run -it -v $HOME/entwine:/opt/data -p 8080:8080 connormanning/greyhound

Now that we have Greyhound running locally and ready to serve our data, we can view it with these `Plasio <http://speck.ly/?s=http://localhost:8080/&r=red-rocks>`_ or `Potree <http://potree.entwine.io/data/custom.html?s=localhost:8080&r=red-rocks>`_ links which point at our local resource.

Going further
--------------------------------------------------------------------------------

Entwine is made to be flexible, and allows for things like configuration templating and supports a variety of behavior controllable via command line or configuration file.  For detailed information about how to configure your builds, check out the `configuration documentation`_.  Here, you can find information about reprojecting your data, producing `Cesium 3D Tiles <https://github.com/AnalyticalGraphicsInc/3d-tiles>`_ output, and all sorts of other settings.

License
--------------------------------------------------------------------------------

Entwine is available under the `LGPL License`_.

.. _`PDAL`: https://pdal.io
.. _`Docker`: http://docker.com

.. _`Greyhound`: https://github.com/hobu/greyhound

.. _`get started with Greyhound`: https://github.com/hobu/greyhound
.. _`configuration documentation`: doc/usage/configuration.md

.. _`LGPL License`: https://github.com/connormanning/entwine/blob/master/LICENSE

