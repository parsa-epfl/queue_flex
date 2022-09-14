#!/bin/bash
# This script activates the virtual environment given as the first parameter, and prepares
# the environment for simulation.

set -x
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
export PYTHONPATH=`pwd`
