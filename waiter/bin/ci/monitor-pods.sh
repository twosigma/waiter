#!/bin/sh

: ${cycle_delay_secs:=${1:-30}}

while true; do
    sleep ${cycle_delay_secs}
    printf '\n***** Kubernetes pod state at %s *****\n%s\n\n' \
        "$(date +'%H:%M:%S')" \
        "$(kubectl --namespace=${USER} get pods 2>&1)"
done
