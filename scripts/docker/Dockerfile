FROM pdal/pdal:1.6
MAINTAINER Connor Manning <connor@hobu.co>

RUN \
    echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories; \
    apk update; \
    apk add --no-cache --virtual .build-deps \
        xz-dev \
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
        openssl-dev \
        openssl \
        cmake \
        libunwind \
        libexecinfo \
    ;\
    git clone https://github.com/connormanning/entwine.git /var/entwine; \
    cd /var/entwine; \
    mkdir build; \
    cd build; \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo .. ; \
    make -j4; \
    make install; \
    rm -rf /var/entwine; \
    apk del .build-deps

ENTRYPOINT ["entwine"]

