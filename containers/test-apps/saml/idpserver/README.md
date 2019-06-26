This is a test SAML identity provider (IdP) server used during the SAML authentication integration test in Waiter.

It was built using the SimpleSAMLphp (https://simplesamlphp.org/) server as a reference for the SAML assertion message format.

## Building

pip install dependencies:

```bash
$ pip3 install -r requirements.txt
```

## Running

```bash
$ ./bin/run-idp-server -p 8080
```
