The jwks-server is a server to help with JWT access token authentication testing.
It supports two endpoints: `get-token` and `keys`.

# Usage

To run the server:
```bash
$ lein run <port> <key-file> <settings-file>
```

## Example:

```bash
$ lein run 8080 resources/jwks.json resources/settings.edn
...
jwks-server.main - command-line arguments: [8080 resources/jwks.json resources/settings.edn]
jwks-server.main - port: 8080
jwks-server.main - jwks file: resources/jwks.json
jwks-server.main - settings file: resources/settings.edn
jwks-server.main - starting server on port 8080
...
eclipse.jetty.server.Server - Started @10846ms
```

# Build Uberjar

```bash
$ lein uberjar
...
Created /path-to-waiter-jwks-server/target/uberjar/jwks-server-0.1.0-SNAPSHOT.jar
Created /path-to-waiter-jwks-server/target/uberjar/jwks-server-0.1.0-SNAPSHOT-standalone.jar
```
