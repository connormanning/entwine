.. image:: ./doc/logo/color/entwine_logo_2-color-small.png

|
|

Entwine is a data organization library for massive point clouds, designed to conquer datasets of hundreds of billions of points as well as desktop-scale point clouds.  Entwine can index anything that is `PDAL`_-readable, and can read/write to a variety of sources like S3 or Dropbox.  Builds are completely lossless, so no points will be discarded even for terabyte-scale datasets.

You can see it in use via the dynamic `Plas.io`_ client at http://speck.ly and the `Potree`_ client at http://potree.entwine.io.

Usage
--------------------------------------------------------------------------------

Getting started with Entwine is easy with `Docker`_.  Pull the most recent image with ``docker pull connormanning/entwine``.  Let's assume there's data at ``~/abc.laz``, and we will output to ``~/entwine/abc``.  Let's build an Entwine index:

::

    mkdir ~/entwine
    docker run -it -v $HOME:/opt/data connormanning/entwine \
        entwine build -i /opt/data/abc.laz -o /opt/data/entwine/abc

|

Now we have Entwine data at ``~/entwine/abc``.  We could have also passed a wildcard input like ``-i /opt/data/directory-of-files/*`` for a larger dataset.  Let's take a look at our index using the `Greyhound`_ Docker image, which we can get with ``docker pull connormanning/greyhound``.  By default, the Greyhound container will search for Entwine data at ``/opt/data`` and listen on port 80, so let's make some port/volume mappings and we should be close to viewing our Entwine output.

::

    docker run -it -v $HOME/entwine:/opt/data -p 8080:80 connormanning/greyhound

|

You may need to forward port 8080 from your docker virtual machine to your host OS; `this post <https://jlordiales.me/2015/04/02/boot2docker-port-forward/>`_ describes one way to do this.
For the impatient, use this one-liner:

::

    VBoxManage controlvm default natpf1 "greyhound,tcp,127.0.0.1,8080,,8080"

|

Now we have Greyhound ready to serve our data, we just need a client renderer to view it.  Let's hit the URL below, which will connect to our new local Greyhound resource.

http://speck.ly?s=http://localhost:8080/&r=abc

Now we should be viewing our dataset dynamically with progressive level-of-detail.  Another client sample would be `Potree`_, which we can try out with http://potree.entwine.io/data/custom.html?s=localhost:8080&r=abc.

Going further
--------------------------------------------------------------------------------

The default settings are fine for many datasets, but Entwine provides many parameters to more finely control your indexing configuration.  Check out ``docker run -it connormanning/entwine entwine build`` for a list of command line overrides.  We've already seen ``-i`` and ``-o``.  Some other highlights:

- ``-r``: Specify a dataset reprojection.  If your data has no color and you reproject to ``EPSG:3857``, speck.ly can overlay imagery tiles in real-time.
- ``-t``: Specify the number of indexing threads.  The default is 9.  If you have a monster machine like an EC2 c3.8xlarge instance, you can try 30.  We recommend a number close to, but no greater than, the number of physical cores on the machine for optimal performance.
- ``-f``: Want to write a new dataset at the same location as a previous one?  This option forces an overwrite.

|

If the command line overrides are insufficient, you can get developer-level control over the indexing parameters by passing a JSON configuration file, for example ``entwine build config.json``.  There is a commented template for this configuration file `here <https://raw.githubusercontent.com/connormanning/entwine/master/template.json>`_.

License
--------------------------------------------------------------------------------

Entwine is available under the `LGPL License`_.

.. _`PDAL`: http://pdal.io
.. _`Docker`: http://docker.com
.. _`Greyhound`: https://github.com/hobu/greyhound
.. _`Plas.io`: http://speck.ly
.. _`Potree`: http://potree.org
.. _`LGPL License`: https://github.com/connormanning/entwine/blob/master/LICENSE
