The token-syncer is a command-line application designed specifically for syncing tokens across Waiter clusters.
This application can be set up as a cron job to sync tokens on independently running Waiter clusters in different datacenters.
Syncing tokens across datacenters facilitates the hosting of highly available services across multiple Waiter clusters.
All that is needed in the datacenter failure scenario is to point the DNS records to an active datacenter running a Waiter instance.
As requests come into the available Waiter cluster, Waiter will spin up service instances on the new datacenter and process the requests.


# Usage

To display usage instructions:
```bash
$ lein run --help
```

To sync tokens across multiple clusters, use comma separated names as follows:
```bash
$ lein run http://c1.localtest.me:9091 http://c2.localtest.me:9093
...
syncing tokens on clusters: \#{http://c1.localtest.me:9091 http://c2.localtest.me:9093}
...
completed syncing tokens
{:details ...
 :summary {:sync {:failed ...,
                  :unmodified ...,
                  :updated ...},
           :tokens {:num-processed ..., :total ...}}} }
exiting with code 0
```

Alternatively, it can also be run after building the uberjar (see below):
```bash
$ java -jar /path/to/token-syncer.jar http://c1.localtest.me:9091 http://c2.localtest.me:9093
...
syncing tokens on clusters: \#{http://c1.localtest.me:9091 http://c2.localtest.me:9093}
...
completed syncing tokens
{:details ...
 :summary {:sync {:failed ...,
                  :unmodified ...,
                  :updated ...},
           :tokens {:num-processed ..., :total ...}}} }
exiting with code 0
```

# Implementation

The token syncer communicates with Waiter clusters on their respective `/token` endpoints to retrieve and manage individual tokens.
Outline of steps performed by the syncer:
1. Loads all tokens on each cluster.
1. Removes any tokens that have matching etags and are not deleted on all clusters (these tokens are already synced).
1. Determines the latest version of the tokens using the token last updated times.
1. Perform the sync operations using following cases:
    1. Report a conflict in sync if the token roots are different.
    1. Update all clusters with stale tokens to the latest token.
    1. Hard-deletes tokens on all clusters once all clusters agree that a token has been soft-deleted.

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
$ WAITER_URIS="http://c1.localtest.me:9091,http://c2.localtest.me:9093" lein test :integration
```
