#!/bin/bash

set -x

################################
# Python environment setup

pyenv global 3.6

python --version
python3 --version

pip install -r requirements_test.txt

################################
# SSL certificate setup

source ./bin/ci/ssl-env.sh

# Generate CA certificate for self-signing
openssl genrsa -out $CA_KEY_PATH 4096
openssl req -new -x509 -key $CA_KEY_PATH -out $CA_CERT_PATH -subj '/CN=Kitchen Test CA/O=Waiter Kitchen/C=US'

# Generate self-signed certificate for HTTPS server
openssl req -newkey rsa:4096 -keyout $KEY_PATH -passout "pass:$KEY_PASSWORD" -out temp.csr -subj '/CN=localhost/O=Waiter Kitchen/C=US'
openssl x509 -req -in temp.csr -CA $CA_CERT_PATH -CAkey $CA_KEY_PATH -CAcreateserial -out $CERT_PATH -extfile <(printf "subjectAltName=DNS:localhost")

# Set up current directory for certificate verification
# (needed for the Python requests verify='.' option)
c_rehash .
