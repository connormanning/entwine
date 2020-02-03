#!/bin/bash

# enable conda for this shell
. /opt/conda/etc/profile.d/conda.sh

# activate the environment
conda activate base

# exec the cmd/command in this process, making it pid 1
exec "$@"
