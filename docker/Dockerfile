# =========================================== BASE =============================================
FROM condaforge/miniforge3:4.10.3-7 as base

ARG USER=thunder
ARG UID=1000
ARG GID=100
ARG HOME=/home/$USER

ENV CONDA_DIR=/opt/conda \
    SHELL=/bin/bash \
    USER=$USER \
    UID=$UID \
    GID=$GID \
    HOME=$HOME

COPY docker/fix-permissions /usr/local/bin/fix-permissions
RUN chmod a+rx /usr/local/bin/fix-permissions

RUN useradd -m -s /bin/bash -N -u $UID $USER && \
    chown -R $USER:$GID $CONDA_DIR && \
    chmod g+w /etc/passwd && \
    fix-permissions $HOME $CONDA_DIR

USER $USER

RUN conda install --quiet --yes \
        'aiofiles==0.7.0' \
        'boto3==1.18.63' \
        'cachetools==4.2.4' \
        'httpx==0.20.0' \
        'python-json-logger==2.0.1' \
        'pyyaml==6.0' \
        'quart==0.15.1' \
        'watchtower==1.0.6' \
        'web3==5.24.0' && \
    conda clean --all -f -y && \
    fix-permissions $HOME $CONDA_DIR

RUN pip install --no-cache-dir \
        'python-binance==1.0.15' \
        'https://github.com/fabio-nukui/cosmos.py/releases/download/v2.0.0-beta9/cosmos_sdk-2.0.0b9-py3-none-any.whl' && \
    fix-permissions $HOME $CONDA_DIR

ENV PATH="${HOME}/.local/bin:${PATH}"

ENV PYTHONPATH="${HOME}/work/src"

RUN mkdir ${HOME}/work

WORKDIR ${HOME}/work

# =========================================== PROD =============================================
FROM base as prod

COPY --chown=$USER:$GID logging_config.yaml ${HOME}/work/

COPY --chown=$USER:$GID resources ${HOME}/work/resources

COPY --chown=$USER:$GID src ${HOME}/work/src

COPY --chown=$USER:$GID docker/git_commit ${HOME}/work/git_commit

# =========================================== MAIN =============================================
FROM prod as main

COPY --chown=$USER:$GID app/main.py ${HOME}/work/app.py

CMD ["python", "app.py"]

# ==================================== TERRA_BROADCASTER =======================================
FROM prod as broadcaster

COPY --chown=$USER:$GID app/broadcaster.py ${HOME}/work/

COPY --chown=$USER:$GID config/broadcaster ${HOME}/work/

CMD ["hypercorn", "--config", "hypercorn.toml", "broadcaster:app", "--certfile", "server.pem", "--keyfile", "server.key"]

# =========================================== DEV ==============================================
FROM base as dev

USER root

RUN apt-get -q update && \
    apt-get install -yq \
        less && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER $USER

RUN conda install --quiet --yes \
        'black==21.9b0' \
        'flake8==4.0.1' \
        'ipython==7.28.0' \
        'isort==5.9.3' \
        'line_profiler=3.3.1' \
        'mypy==0.910' \
        'pytest==6.2.5' \
        'types-aiofiles' \
        'types-cachetools' \
        'types-PyYAML' && \
    conda clean --all -f -y  && \
    fix-permissions $HOME $CONDA_DIR

# =========================================== LAB ==============================================
FROM dev as lab

RUN conda install --quiet --yes \
        'bottleneck' \
        'dtale' \
        'ipywidgets' \
        'ipympl' \
        'jupyterlab' \
        'jupyterlab_execute_time' \
        'matplotlib' \
        'nodejs' \
        'numexpr' \
        'pandas' \
        'plotly' \
        'qgrid' && \
    conda clean --all -f -y  && \
    npm cache clean --force  && \
    jupyter notebook --generate-config  && \
    jupyter lab clean && \
    rm -rf $HOME/.cache/yarn && \
    fix-permissions $HOME $CONDA_DIR

RUN jupyter labextension install -y \
        jupyterlab-chart-editor \
        jupyterlab-kernelspy \
        jupyterlab-plotly \
    jupyter lab clean && \
    jlpm cache clean && \
    npm cache clean --force && \
    rm -rf $HOME/.node-gyp && \
    rm -rf $HOME/.local && \
    fix-permissions $HOME $CONDA_DIR

COPY config/lab/jupyter_lab_config.py $HOME/.jupyter/

COPY config/lab/tracker.jupyterlab-settings $HOME/.jupyter/lab/user-settings/@jupyterlab/notebook-extension/

COPY config/lab/shortcuts.jupyterlab-settings $HOME/.jupyter/lab/user-settings/@jupyterlab/shortcuts-extension/

USER root

RUN fix-permissions ${HOME}/.jupyter

USER $USER
