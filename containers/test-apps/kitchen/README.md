The kitchen app is an HTTP server designed specifically for exercising test scenarios in Waiter.
The [Waiter integration tests](../waiter/integration) rely heavily on kitchen for verifying that Waiter behaves as expected in various situations.

# Requirements

Kitchen should run on any system with Python 3.4 (or newer) installed as the default `python3` binary.

# Manual Testing

```bash
$ ./bin/kitchen --port PORT

$ curl -XPOST $(hostname):PORT
Hello World

$ curl -XPOST -H "x-kitchen-echo;" -d "some text I want back" $(hostname):PORT
some text I want back

$ curl -v -XPOST -H "x-kitchen-cookies: a=b,c=d" $(hostname):PORT
...
< HTTP/1.1 200 OK
< Set-Cookie: a=b
< Set-Cookie: c=d
...

$ telsocket -url="ws://$(hostname):${PORT}/ws-test"
Connected!

 Connected to kitchen
some text I want back

 some text I want back
chars-10

 FIMCEWPBMR
chars-10

 URSQQYTBQL
^C
```

# Automated Integration Tests

## Requirements

Although Kitchen itself is a standalone script that should run on any Python 3 version,
the integration tests require Python 3.6 or better, and also require some extra packages.
The testing dependencies are listed in `requirements_test.txt`.

## Running

We use Pytest for our integration test suite.
A Kitchen server is automatically started as part of the test suite setup.

```bash
$ pip install -r requirements_test.txt
...

$ pytest
...
```

Alternatively, you may test against a manually-started kitchen server by setting some environment variables:

```bash
$ ./bin/kitchen -p 8080 &
...

$ KITCHEN_AUTOSTART=false KITCHEN_HOST=localhost KITCHEN_PORT=8080 pytest
...
```

# Testing in Waiter

For convenience, a Waiter token specification for the Kitchen app is included in `kitchen.json`.
This makes it simple to start up an instance of Kitchen using the Waiter CLI:

```bash
$ waiter create --json ./kitchen.json
Attempting to create token on dev0...
Successfully created kitchen.
$ waiter ping kitchen
Pinging token kitchen at /status in dev0...
Ping successful.
Hello World
```
