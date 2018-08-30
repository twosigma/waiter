#!/bin/bash

set -x

source ./bin/ci/ssl-env.sh

python --version
pytest --version

pytest
