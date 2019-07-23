.. _home:

.. image:: ../logo/color/entwine_logo_2-color.png
   :alt: Entwine logo


Entwine is a data organization library for massive point clouds, designed to conquer datasets of trillions of points as well as desktop-scale point clouds. Entwine can index anything that is `PDAL`_-readable, and can read/write to a variety of sources like S3 or Dropbox. Builds are completely lossless, so no points, metadata, or precision will be discarded even for terabyte-scale datasets.

Check out the client demos, showcasing Entwine output with `Potree`_, `Plasio`_, and `Cesium`_ clients.

Entwine is available under the `LGPL License`_.

.. _`PDAL`: http://pdal.io
.. _`Plasio`: http://dev.speck.ly
.. _`Potree`: http://potree.entwine.io
.. _`Cesium`: http://cesium.entwine.io
.. _`LGPL License`: https://github.com/connormanning/entwine/blob/master/LICENSE

******************************************************************************
Entwine
******************************************************************************

News
--------------------------------------------------------------------------------

**2019-07-23**
................................................................................

Entwine 2.1 is now released. See :ref:`download` to obtain a copy of the source
code.

**2018-12-18**
................................................................................

Entwine 2.0 is now released. See :ref:`download` to obtain a copy of the source
code.

**2017-12-01**
................................................................................

See `Connor Manning`_ present "`Trillions of points - spatial indexing, organization, and exploitation of massive point clouds`_" at `FOSS4G 2017`_ in Boston, MA USA in August 2017.

.. _`Trillions of points - spatial indexing, organization, and exploitation of massive point clouds`: https://vimeo.com/245073446
.. _`FOSS4G 2017`: http://2017.foss4g.org
.. _`Connor Manning`: https://github.com/connormanning

.. toctree::
   :maxdepth: 1
   :hidden:

   quickstart
   download
   configuration
   entwine-point-tile
   community
   presentations

.. _`point cloud data`: http://en.wikipedia.org/wiki/Point_cloud

