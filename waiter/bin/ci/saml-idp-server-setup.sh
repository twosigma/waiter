#!/bin/bash

set -e

# Run a test SAML identity provider (IdP) server via docker
# Server UI will be accessible at: https://localhost:8443/simplesaml/module.php/core/frontpage_welcome.php
# SAML authentication request can be routed to: https://localhost:8443/simplesaml/saml2/idp/SSOService.php
# Further documentation can be found at: https://hub.docker.com/r/kristophjunge/test-saml-idp/

echo Starting SAML IdP server docker container
docker run --name=testsamlidp_idp --detach --rm -p 8090:8090 -p 8443:8443 \
-e SIMPLESAMLPHP_SP_ENTITY_ID=waiter \
-e SIMPLESAMLPHP_SP_ASSERTION_CONSUMER_SERVICE=http://localhost:9091/waiter-auth/saml/acs \
-e SIMPLESAMLPHP_SP_SINGLE_LOGOUT_SERVICE=http://localhost:9091/waiter-auth/saml/logout \
-d kristophjunge/test-saml-idp:1.15

echo -n Waiting for SAML IdP server
while ! curl -k https://localhost:8443/simplesaml/saml2/idp/metadata.php &>/dev/null; do
    echo -n .
    sleep 3
done
echo
echo -n SAML IdP server started successfully
echo
