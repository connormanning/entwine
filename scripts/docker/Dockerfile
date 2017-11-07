FROM pdal/pdal:1.5
MAINTAINER Connor Manning <connor@hobu.co>

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    cmake \
    liblzma-dev \
    libjsoncpp-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    vim

ENV CC gcc
ENV CXX g++

ADD https://api.github.com/repos/connormanning/entwine/commits /tmp/bust-cache
RUN git clone https://github.com/connormanning/entwine.git /var/entwine && \
    cd /var/entwine && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
    make -j4 && \
    make install

ENTRYPOINT ["entwine"]

