# Before You Start

Check out [Waiter Checklist](service-checklist.md) before you try running your service on Waiter.

# Overview

There are two ways to launch services on Waiter: the On-the-Fly API and the Token API.  The On-the-Fly API automatically starts HTTP services using custom HTTP headers coming from the client.  Alternatively, the Token API allows service maintainers to predefine a service.  Token-based services are reachable by HTTP clients using a DNS name or using a custom HTTP header, `x-waiter-token`.

## On-the-Fly API

If you have a service that speaks HTTP, Waiter can run it for you. The following is an example of how to start your service in Waiter using `curl`.

### Example
```
$ curl -s -X POST \
    -H "x-waiter-cpus: 1" \
    -H "x-waiter-mem: 1024" \
    -H "x-waiter-version: v1" \
    -H "x-waiter-cmd: bin/kitchen -p \$PORT0" \
    https://<waiter-uri>/hello
Hello World
```

#### Explanation

Upon receiving this request, Waiter will inspect the custom Waiter headers and, based upon those headers, Waiter will allocate 1 core and 1024MB of RAM for the service, and launch it. Once the service is up and running, Waiter will forward the `POST` request to the service's `/hello` endpoint. Waiter will automatically maintain enough instances of the service to meet demand from clients. By default, Waiter will launch the service as the authenticating user. This can be changed by setting the `X-Waiter-Run-As-User` header (see [Parameters](parameters.md) for more information).

### Getting Started

When using the On-the-Fly API, Waiter's behavior is customized through HTTP headers sent from the HTTP client.  Headers that Waiter supports are explained [here](parameters.md). HTTP clients make requests to your service using `https://<waiter-uri>` as the base of your service's URL. Paths appended to this base URL will be sent to your service.

## Token API

The Token API allows clients to reach Waiter-hosted services by providing a token.  The token can be a DNS name, which allows usage of Waiter without any custom HTTP headers.

### Define the Service

Create a file called `kitchen.json`.  For this example, "kitchen" was chosen as the token.  This token will be used later to make a request to the service. Place the following in `kitchen.json`:

```
{
 "name": "kitchen",
 "cpus": 1,
 "mem": 512,
 "version": "v1",
 "cmd": "bin/kitchen -p $PORT0",
 "health-check-url": "/status",
 "token": "kitchen",
 "permitted-user": "*"
}
```

### Register the Service

Tell Waiter about this service:

```
$ curl -s -H "Accept: application/json" -X POST https://<waiter-uri>/token -d@kitchen.json
```

### Test the Service

Now that the service is defined and registered, test it out using the "kitchen" token:

```
$ curl -s -H "x-waiter-token: kitchen" -X POST https://<waiter-uri>/hello
Hello World
```

## Using DNS Instead of Tokens

Instead of passing the token as a header via `x-waiter-token`, the host name can act as the token.  In the following example, the token is updated to a valid DNS name, `kitchen.app.example.com`.

### Create the DNS record

You'll need to create a DNS entry for your service that maps your alias (e.g. `kitchen.app.example.com`) to your Waiter hostname. The specifics of how to do this will be different from one environment to the next.

### Register the Service

Use the following token definition:

```
{
 "name": "kitchen",
 "cpus": 1,
 "mem": 512,
 "version": "v1",
 "cmd": "bin/kitchen -p $PORT0",
 "health-check-url": "/status",
 "token": "kitchen.app.example.com",
 "permitted-user": "*"
}
```

### Test the Service

```
$ curl -s -X POST http://kitchen.app.example.com/hello
Hello World
```

Currently HTTPS is not supported for DNS-based token requests.

## Combining the Token and On-The-Fly APIs

The Token and On-The-Fly APIs can be combined to enable more usage scenarios.  For example, you could define a token that contains all parameters except for the version, and provide the version via a header:

```
$ curl -s -H "x-waiter-token: kitchen" \
          -H "x-waiter-version: v2" \
          -X POST https://<waiter-uri>/hello
Hello World
```

If a parameter is defined via a Waiter header as well as in the token, the header parameter takes precedence.

# Next Steps

- Check out the [Waiter Checklist](service-checklist.md) if you haven't already.
- Tune how Waiter processes requests for your service using [custom parameters](parameters.md).
