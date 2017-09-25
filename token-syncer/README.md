The token-syncer app is a command-line application designed specifically for syncing tokens across Waiter clusters.

# Implementation

The token syncer communicates with Waiter clusters on their respective `/token` endpoints to retrieve and manage individual tokens.
When requested to sync tokens across multiple clusters, it first loads all tokens on each cluster (clusters are specified as a query parameter).
It then computes the latest version of the tokens using the `last-update-time` field in the token description.
It then goes ahead to perform the sync operations using following cases:
1. Updates the token description on the other clusters if any of the clusters do not agree on the token description.
2. Hard-deletes a token if all the clusters agree on the token description and the token has been soft-deleted.

# Build Uberjar

```bash
$ lein uberjar
...
Created /path-to-waiter-token-syncer/target/uberjar/token-syncer-0.1.0-SNAPSHOT.jar
Created /path-to-waiter-token-syncer/target/uberjar/token-syncer-0.1.0-SNAPSHOT-standalone.jar
```

# Running Tests

## Unit testing
```bash
$ lein test
```

## Integration testing

Ensure you have multiple Waiters running, e.g. locally on port 9091 and 9093
```bash
$ WAITER_URI http://127.0.0.1:9091;http://127.0.0.1:9093 lein test :integration
```
