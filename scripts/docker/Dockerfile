FROM connormanning/pdal:master
MAINTAINER Connor Manning <connor@hobu.co>

RUN apt-get update && apt-get install -y \
    autoconf \
    build-essential \
    cmake \
    git \
    liblzma-dev \
    libjsoncpp-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    ninja-build \
    python-numpy \
    python-pip \
    vim && \
    pip install numpy

ENV CC gcc
ENV CXX g++

RUN git clone https://github.com/jemalloc/jemalloc.git /var/jemalloc && \
    cd /var/jemalloc && \
    ./autogen.sh && \
    make dist && \
    make && \
    make install
ENV LD_PRELOAD /usr/local/lib/libjemalloc.so.2

ARG branch=master
RUN echo Branch: $branch
ADD https://api.github.com/repos/connormanning/entwine/commits?sha=$branch \
    /tmp/bust-cache

RUN git clone https://github.com/connormanning/entwine.git /var/entwine && \
    cd /var/entwine && \
    git checkout $branch && \
    mkdir build && \
    cd build && \
    cmake -G Ninja \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    ninja && \
    ninja install

ENTRYPOINT ["entwine"]

