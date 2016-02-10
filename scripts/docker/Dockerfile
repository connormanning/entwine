FROM pdal/master:latest
MAINTAINER Connor Manning <connor@hobu.co>

ENV CC clang
ENV CXX clang++

RUN mkdir -p /root/.ssh
COPY entwine-deploy.key /root/.ssh/entwine_key
COPY ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/entwine_key

RUN git clone git@entwine:connormanning/entwine.git && \
    cd entwine && \
    mkdir build && \
    cd build && \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    make -j4 && \
    make install

