FROM continuumio/miniconda

RUN . /opt/conda/etc/profile.d/conda.sh && conda install --yes --freeze-installed \
    -c conda-forge \
    nomkl \
    entwine \
    python=3.8 \
    && conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete


COPY entrypoint.sh /entrypoint
USER root
RUN chmod +x /entrypoint
ENTRYPOINT ["/entrypoint"]


