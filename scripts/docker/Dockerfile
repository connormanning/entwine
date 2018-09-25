FROM pdal/ubuntu:master
MAINTAINER Connor Manning <connor@hobu.co>

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    liblzma-dev \
    libjsoncpp-dev \
    libcurl4-openssl-dev \
    curl \
    libssl-dev \
    python-numpy \
    python-pip \
    vim && \
    pip install numpy

ENV CC gcc
ENV CXX g++

ADD https://api.github.com/repos/connormanning/entwine/commits?sha=optimize \
    /tmp/bust-cache

RUN git clone https://github.com/connormanning/entwine.git /var/entwine && \
    cd /var/entwine && \
    git checkout format && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    make -j4 && \
    make install

ENTRYPOINT ["entwine"]

