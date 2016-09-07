FROM pdal/pdal:1.3
MAINTAINER Connor Manning <connor@hobu.co>

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    cmake \
    clang \
    liblzma-dev \
    libjsoncpp-dev \
    libcurl4-openssl-dev

RUN apt-get install -y

ENV CC clang
ENV CXX clang++

ADD http://www.timeapi.org/utc/now /tmp/bust-cache
RUN git clone https://github.com/connormanning/entwine.git && \
    cd entwine && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    make -j4 && \
    make install

