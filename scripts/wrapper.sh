#!/bin/bash

# The Cuttlefish wrapper requires Apptainer and a modern version of
# Python, ideally 3.11+, to operate correctly. Modify this for your HPC
# cluster's module system and Python setup.

module load gcc apptainer python

# MPI networking can be a bit iffy when running inside Apptainer. If you 
# encounter issues, try modifying these environment variables.

export PMIX_MCA_psec=^munge
export OMPI_MCA_btl=^openib

# Change to the Cuttlefish directory
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
source base_conf.sh

source $CF_TMP_BASE/$CF_VENV_NAME/bin/activate

echo "Command:" "$@" >> $CF_LOG_DIR/cfdep-$(hostname).log
exec "$@"