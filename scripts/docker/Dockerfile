FROM connormanning/entwine:dependencies
MAINTAINER Connor Manning <connor@hobu.co>

RUN cd /opt/ && \
    wget https://ftp.gnu.org/gnu/time/time-1.9.tar.gz && \
    tar zxvf time-1.9.tar.gz && \
    cd time-1.9 && \
    ./configure && \
    make && \
    make install && \
    cp time /usr/bin/time

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

RUN mkdir /entwine
RUN chmod -R go+rwX /entwine

RUN useradd --create-home --no-log-init --shell /bin/bash entwine
USER entwine
RUN cd $HOME

ENTRYPOINT ["/usr/bin/time", "-v", "entwine"]
