#!/bin/bash

set -ex

#
# Run Kitchen integration tests
#

source ./bin/ci/ssl-env.sh

python --version
pytest --version

pytest
