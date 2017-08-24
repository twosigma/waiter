#!/usr/bin/env bash
# Usage: add-loopback-ips-mac.sh
#
# Examples:
#   add-loopback-ips-mac.sh
#
# Aliases 127.0.0.[2-10] address to lo0, which is needed to use shell scheduler locally.
# This script is for Mac OS X.  Linux binds the necessary loopback IPs by default.

set -e

if [ $USER != "root" ]; then
    echo "This script must be run as root (sudo)." 1>&2
    exit 1
fi

for i in $(seq 2 10); do
    echo "Aliasing 127.0.0.$i to lo0..."
    /sbin/ifconfig lo0 alias 127.0.0.$i up
done

echo "Done."
