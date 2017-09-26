The token-syncer is a command-line application designed specifically for syncing tokens across Waiter clusters.
This application can be set up as a cronjob to run at intervals to sync tokens on independently running Waiter clusters on different datacenters.
The syncing allows supporting datacenter failures as we can failover to another datacenter either with minimal downtime and customer impact.
All that is needed in the datacenter failure scenario is to point the DNS records to an active datacenter running a Waiter instance.
As requests come in to the new Waiter instance, Waiter will spin up service instances on the new datacenter and process the requests.


# Usage

The token-syncer is a command-line application written in Clojure.
It can be run as follows:

```bash
$ lein run --help
<displays usage instructions>
```

```bash
$ lein run --cluster-urls "http://cluster-1.localtest.me:9091;http://cluster-2.localtest.me:9093"
...
Syncing tokens on clusters: \#{http://cluster-1.localtest.me:9091 http://cluster-2.localtest.me:9093}
...
Exiting.
```

Alternatively, it can also be run after building the uberjar (see below):
```bash
$ java -jar /path/to/token-syncer.jar --cluster-urls "http://cluster-1.localtest.me:9091;http://cluster-2.localtest.me:9093"
...
Syncing tokens on clusters: \#{http://cluster-1.localtest.me:9091 http://cluster-2.localtest.me:9093}
...
Exiting.
```

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
