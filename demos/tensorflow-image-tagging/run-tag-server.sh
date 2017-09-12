#!/usr/bin/env bash
# Usage: run-tag-server.sh.sh [PORT]

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PORT=${1:-8080}

echo "tag server executable location: ${DIR}"
cd ${DIR}

echo "tag server will launch at port ${PORT}"
python3 tag-server.py ${PORT}