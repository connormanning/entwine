FROM pdal/ubuntu:latest
MAINTAINER Connor Manning <connor@hobu.co>

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    cmake \
    liblzma-dev \
    libjsoncpp-dev \
    libcurl4-gnutls-dev \
    libssl1.0-dev \
    vim

ENV CC gcc
ENV CXX g++

ADD https://api.github.com/repos/connormanning/entwine/commits?sha=ept \
    /tmp/bust-cache

RUN git clone https://github.com/connormanning/entwine.git /var/entwine && \
    cd /var/entwine && \
    git checkout master && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    make -j4 && \
    make install

ENTRYPOINT ["entwine"]

