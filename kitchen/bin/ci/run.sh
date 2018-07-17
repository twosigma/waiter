#!/bin/bash

set -x

python --version
pytest --version

pytest
