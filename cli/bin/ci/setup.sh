#!/usr/bin/env bash

set -ev

################################
# Python environment setup

pyenv global 3.6

python3 --version

# Explicitly uninstall cli
if [[ $(pip list --format=columns | grep waiter-client) ]];
then
    pip uninstall -y waiter-client
fi

pip install -e .
pip install -r integration/requirements.txt
