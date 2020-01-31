#FROM continuumio/miniconda3:4.7.12-alpine
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
#RUN chown anaconda /entrypoint
ENTRYPOINT ["/entrypoint"]


#FROM continuumio/miniconda as builder
#MAINTAINER Connor Manning <connor@hobu.co>
#
#ARG conda_env=entwine
#ARG py_ver=3.8
#

#RUN conda update conda --yes
#RUN conda --version
#
#RUN conda create -n ${conda_env} -c conda-forge entwine \
#    && conda clean --all -f -y
#
#
#ENV PATH $CONDA_DIR/envs/${conda_env}/bin:$PATH
#ENV CONDA_DEFAULT_ENV ${conda_env}
#
#ENTRYPOINT ["conda run -n entwine entwine"]

