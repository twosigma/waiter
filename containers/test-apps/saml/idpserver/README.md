This is a test SAML identity provider (IdP) server used during the SAML authentication integration test in Waiter.

It was built using the SimpleSAMLphp (https://simplesamlphp.org/) server as a reference for the SAML assertion message format.

## Building

pip install dependencies:

```bash
$ pip3 install -r requirements.txt
```

## Running

Run ```bin/run-idp-server```; see file for usage details.

```bash
$ ./bin/run-idp-server 8000 https://127.0.0.1/waiter-auth/saml/acs http://127.0.0.1:9091/waiter-auth/saml/acs myusername
```
