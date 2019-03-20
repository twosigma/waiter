#!/usr/bin/env bash

set -ev

################################
# Python environment setup

apt-get update && apt-get install -y python3

pyenv install 3.6.3
pyenv global 3.6.3

python3 --version

# Explicitly uninstall cli
if [[ $(pip list --format=columns | grep waiter-client) ]];
then
    pip uninstall -y waiter-client
fi

pip install -e .
pip install -r integration/requirements.txt

# Make sure that pyinstaller can successfully build the cli
pip install pyinstaller
pyinstaller --clean --onefile --name waiter waiter/__main__.py
./dist/waiter
