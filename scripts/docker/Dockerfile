FROM connormanning/pdal:entwine-pin
MAINTAINER Connor Manning <connor@hobu.co>

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    cmake \
    liblzma-dev \
    libjsoncpp-dev \
    libcurl4-openssl-dev

ENV CC gcc
ENV CXX g++

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

