#!/bin/bash

export TMPDIR=/tmp/$(id -u)
export CF_PIP_CACHE_DIR=/tmp/$(id -u)/pip-cache
export CF_HOSTNAME=$(hostname)
export CF_TMP_BASE=$TMPDIR/$CF_HOSTNAME
export CF_VENV_NAME="cuttlefish-venv"
export CF_LOG_DIR="../logs"