FROM condaforge/mambaforge:latest as build
MAINTAINER Connor Manning <connor@hobu.co>

ARG branch=master
RUN echo Branch: $branch
ADD https://api.github.com/repos/connormanning/entwine/commits?sha=$branch \
    /tmp/bust-cache


SHELL ["conda", "run", "-n", "base", "/bin/bash", "-c"]

RUN mamba install -c conda-forge ninja cmake compilers
RUN mamba install -c conda-forge entwine --only-deps

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

