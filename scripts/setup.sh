#!/bin/bash

# Modify this script based on your HPC cluster's module system and
# Python setup.  

# WARNING: If your home directory is on a network filesystem, like in
# most HPC cluster setups, you MUST change pip's cache directory to a
# local path! Pip will break when multiple instances try to write to the
# same cache files! The script is already configured to use /tmp for its
# working and cache directories - change this to a different local path
# if needed.

# pip config set global.cache-dir /tmp/$(id -u)/pip-cache

# Sometimes the value for TMPDIR is set to a non-local path by the
# cluster environment. You must make sure it points to a local path!

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

source base_conf.sh

module load gcc python

echo "Using temporary directory: $CF_TMP_BASE"
mkdir -p $CF_TMP_BASE/$CF_VENV_NAME 2> /dev/null


echo "Setting up virtual environment in $CF_TMP_BASE/$CF_VENV_NAME"
rm -r $CF_TMP_BASE/$CF_VENV_NAME 2> /dev/null
mkdir -p $CF_TMP_BASE/$CF_VENV_NAME
python3 -m venv $CF_TMP_BASE/$CF_VENV_NAME

echo "Installing required Python packages"
mkdir -p $CF_LOG_DIR
touch $CF_LOG_DIR/cfdep-$(hostname).log
source $CF_TMP_BASE/$CF_VENV_NAME/bin/activate
echo $PWD
pip install --upgrade pip \
    &>> $CF_LOG_DIR/cfdep-$(hostname).log
pip install -r requirements.txt \
    &>> $CF_LOG_DIR/cfdep-$(hostname).log