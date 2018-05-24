FROM pdal/pdal:1.7.1
MAINTAINER Connor Manning <connor@hobu.co>

RUN \
    echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> \
        /etc/apk/repositories; \
    apk update; \
    apk add --no-cache \
        xz-libs \
        cmake \
        libunwind \
        libexecinfo \
    ;

ADD https://www.random.org/cgi-bin/randbyte?nbytes=64&format=h /var/cache.txt

RUN \
    apk add --no-cache --virtual .build-deps \
        xz-dev \
        alpine-sdk \
        git \
        libressl-dev \
        curl-dev \
        libexecinfo-dev \
        libunwind-dev \
        laz-perf-dev \
    ; \
    git clone https://github.com/connormanning/entwine.git /var/entwine; \
    cd /var/entwine; \
    git checkout ept-contained; \
    mkdir -p /usr/local/include; \
    mkdir build; \
    cd build; \
    cmake -G "Unix Makefiles" \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo .. ; \
    make -j4; \
    make install; \
    apk del .build-deps

ENTRYPOINT ["entwine"]

