#!/bin/bash
# Usage: saml-idp-server-setup.sh
#
# Examples:
#   saml-idp-server-setup.sh
#
# Run a dummy SAML identity provider (IdP) server
# SAML authentication request can be routed to: http://localhost:<$SAML_IDP_PORT>/

set -e

sudo apt-get install python3-pip
sudo apt-get install python3-setuptools
sudo -H pip3 install -r ${WAITER_DIR}/../containers/test-apps/saml/idpserver/requirements.txt

echo Starting SAML IdP server
${WAITER_DIR}/../containers/test-apps/saml/idpserver/bin/run-idp-server \
    $SAML_IDP_PORT \
    https://localhost/waiter-auth/saml/acs \
    http://${WAITER_URI}/waiter-auth/saml/acs \
    $(id -un) &

echo -n Waiting for SAML IdP server
while ! curl -k http://localhost:${SAML_IDP_PORT}/healthcheck &>/dev/null; do
    echo -n .
    sleep 3
done
echo
echo -n SAML IdP server started successfully
echo