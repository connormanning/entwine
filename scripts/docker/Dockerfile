ARG BASE_IMAGE="ubuntu:22.04"

FROM --platform=$TARGETPLATFORM condaforge/mambaforge:latest as build
MAINTAINER Connor Manning <connor@hobu.co>

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}" \
    && printf ", TARGETARCH=${TARGETARCH}" \
    && printf ", TARGETVARIANT=${TARGETVARIANT} \n" \
    && printf "With uname -s : " && uname -s \
    && printf "and  uname -m : " && uname -m

#ARG branch=master

#RUN echo Branch: $branch
#ADD https://api.github.com/repos/connormanning/entwine/commits?sha=$branch \
#    /tmp/bust-cache

RUN mamba install -c conda-forge conda-pack -y
RUN mamba create -n build -y -c conda-forge ninja cmake compilers
RUN mamba create -n entwine -y

SHELL ["conda", "run", "-n", "build", "/bin/bash", "-c"]

RUN mamba install -y -c conda-forge entwine --only-deps



SHELL ["conda", "run", "-n", "base", "/bin/bash", "-c"]

RUN mamba install -c conda-forge ninja cmake compilers
RUN mamba install -c conda-forge entwine --only-deps

RUN git clone https://github.com/connormanning/entwine.git /var/entwine && \
    cd /var/entwine && \
    git checkout $branch && \
    mkdir build && \
    cd build && \
    cmake -G Ninja \
        -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
        -DCMAKE_BUILD_TYPE=Release .. && \
    ninja && \
    ninja install

SHELL ["conda", "run", "-n", "entwine", "/bin/bash", "-c"]

ENV CONDA_ENV_NAME "entwine"
ENV CONDAENV "/opt/conda/envs/${CONDA_ENV_NAME}"

RUN mamba install -c conda-forge entwine --only-deps
RUN cp "/opt/conda/envs/build/bin/entwine" "$CONDA_PREFIX/bin/entwine"
RUN ls "/opt/conda/envs/build/lib/"
RUN cp "/opt/conda/envs/build/lib/libentwine.so" "$CONDA_PREFIX/lib"
RUN cp "/opt/conda/envs/build/lib/libentwine.so.2.2.0" "$CONDA_PREFIX/lib"
RUN cp "/opt/conda/envs/build/lib/libentwine.so.3" "$CONDA_PREFIX/lib"


SHELL ["conda", "run", "-n", "base", "/bin/bash", "-c"]

RUN conda-pack -n ${CONDA_ENV_NAME} --dest-prefix=/var/task -o /tmp/env.tar && \
     mkdir /venv && cd /venv && tar xf /tmp/env.tar  && \
     rm /tmp/env.tar


FROM --platform=$TARGETPLATFORM ${BASE_IMAGE:?} as al2

ARG BASE_IMAGE
ARG TARGETPLATFORM
ARG TARGETARCH

ENV CONDAENV "/var/task"
ENV CONDA_PREFIX "/var/task"
ENV TARGETPLATFORM "${TARGETPLATFORM}"
COPY --from=build /venv ${CONDAENV}


ENV PROJ_LIB ${CONDAENV}/share/proj
ENV PROJ_NETWORK=TRUE
ENV PATH $PATH:${CONDAENV}/bin
ENV LD_LIBRARY_PATH=${CONDAENV}/lib


SHELL [ "/bin/bash", "--login", "-c" ]

ENTRYPOINT ["entwine"]
