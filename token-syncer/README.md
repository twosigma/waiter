The token-syncer app is an HTTP server designed specifically for syncing tokens across Waiter clusters.

# Implementation

The token syncer communicates with Waiter routers on their respective `/token` endpoints to retrieve
and manage individual tokens.
When requested to sync tokens across multiple routers (via the `/sync-tokens` endpoint), it first loads
all tokens on each router (routers are specified as a query parameter).
It then computes the latest version of the tokens using the `last-update-time` field in the token description.
It then goes ahead to perform the sync operations using following cases:
1. Updates the token description on the other routers if any of the routers do not agree on the token description.
2. Hard-deletes a token if all the routers agree on the token description and the token has been soft-deleted.

# Configuration

Please see the [config file](./config-full.edn) for details on how to configure the token-syncer.

# Build Uberjar

```bash
$ lein uberjar
...
Created /path-to-waiter-token-syncer/target/uberjar/token-syncer-0.1.0-SNAPSHOT.jar
Created /path-to-waiter-token-syncer/target/uberjar/token-syncer-0.1.0-SNAPSHOT-standalone.jar
```

# Test

```bash
$ lein run --port PORT

$ curl -XPOST $(hostname):PORT
Hello World

$ curl -XPOST -H "x-token-syncer-echo;" -d "some text I want back" $(hostname):PORT
some text I want back

$ curl -v -XPOST -H "x-token-syncer-cookies: a=b,c=d" $(hostname):PORT
...
< HTTP/1.1 200 OK
< Set-Cookie: a=b
< Set-Cookie: c=d
...
```
