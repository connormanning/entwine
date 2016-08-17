.. _docker:

Docker
================================================================================

.. index:: Docker, software installation



Build
--------------------------------------------------------------------------------

    ::

        $ cd entwine/scripts/docker
        $ docker build -t entwineimage .

Use
--------------------------------------------------------------------------------

    ::

        $ docker run -v `pwd:/data entwineimage entwine /data/myfile.laz /data/output

