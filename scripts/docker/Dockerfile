FROM pdal/pdal:1.6-alpine
MAINTAINER Connor Manning <connor@hobu.co>


RUN \
    echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories; \
    apk update; \
    apk add --no-cache --virtual .build-deps \
        xz-dev \
        jsoncpp-dev \
        openssl-dev \
        alpine-sdk \
        git \
        laz-perf-dev \
        curl-dev \
        libexecinfo-dev \
        libunwind-dev \
    ; \
   apk add --no-cache \
        xz-libs \
        jsoncpp-libs \
        openssl-dev \
        openssl \
        cmake \
        libunwind \
        libexecinfo \
    ;\
# RUN apt-get update && apt-get install -y \
#     git \
#     build-essential \
#     cmake \
#     liblzma-dev \
#    libjsoncpp-dev \
#    libcurl4-openssl-dev \
#    libssl-dev \
#    vim
#
#ENV CC gcc
#ENV CXX g++
#
#ADD https://api.github.com/repos/connormanning/entwine/commits /tmp/bust-cache
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

