#!/usr/bin/env bash

lein with-profiles +test-log test || { echo "unit tests failed -- dumping logs"; tail -n +1 -- log/*.log; exit 1; }
