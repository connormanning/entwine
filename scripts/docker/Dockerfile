FROM connormanning/pdal:entwine-pin
MAINTAINER Connor Manning <connor@hobu.co>

ENV CC clang
ENV CXX clang++

RUN git clone https://github.com/connormanning/entwine.git && \
    cd entwine && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    make -j4 && \
    make install

