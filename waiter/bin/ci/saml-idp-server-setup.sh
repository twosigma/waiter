#!/bin/bash

set -e

# Run SAML idendity provider (IdP) server via docker
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
