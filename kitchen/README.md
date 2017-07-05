The kitchen app is an HTTP server designed specifically for exercising test scenarios in Waiter.
The [Waiter integration tests](../waiter/integration) rely heavily on kitchen for verifying that Waiter behaves as expected in various situations.

# Build Uberjar

```bash
$ lein uberjar
...
Created /path-to-waiter-kitchen/target/uberjar/kitchen-0.1.0-SNAPSHOT.jar
Created /path-to-waiter-kitchen/target/uberjar/kitchen-0.1.0-SNAPSHOT-standalone.jar
```

# Test

```bash
$ lein run --port PORT

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
```
