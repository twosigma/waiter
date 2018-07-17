#!/bin/bash

set -x

pyenv global 3.6

python --version
python3 --version

pip install -r requirements_test.txt
