# Service References

To process a http request, Waiter needs to resolve a request to a service description.
This service description tells Waiter how to locate the service instance to use to process the current request.

Waiter uses request headers to construct the service description using the `ServiceDescriptionBuilder`.
It is possible for the service to be reachable from various combinations of headers:
  1. token - single or multiple
  1. on-the-fly services without tokens
  1. on-the-fly services that extend tokens, e.g. run-as-requester

## Service GC

By default, Waiter relies on the service idle timeout to GC services after periods of inactivity (not receiving requests).
However, services reachable only via references, e.g. tokens, can be GC-ed eagerly if the reference has been updated.

When a service description is constructed from a request, the service references are also updated.
These references are available as the `:reference-type->entry` key in the descriptor.
The `reference-type->entry` is a map where the keys represent a `:type` parameter.

### Reference (`:reference-type->entry` in the descriptor) examples

- A service created with on-the-fly header without tokens will have an empty map
  as the following value for `:reference-type->entry` in the descriptor:
```
  {}
```

- A service created with `x-waiter-token: foo` header on a request will have the
  following value for `:reference-type->entry` in the descriptor:
```
  {:token {:sources [{:token "foo" :version "v0"}]}}
```

- A service created with `x-waiter-token: bar,baz` header on a request will have the
  following value for `:reference-type->entry` in the descriptor:
```
  {:token {:sources [{:token "bar" :version "v1"}
                     {:token "baz" :version "v2"}]}}
```

If the same service can be accessed by another token, we end up building multiple
  `reference-type->entry` maps as references that refer to the service.
E.g. if all the above requests mapped to the same service, that service would have
  the following references:
```
  #{{}
    {:token {:sources [{:token "foo" :version "v0"}]}}
    {:token {:sources [{:token "bar" :version "v1"}
                       {:token "baz" :version "v2"}]}}}
```

### Staleness checks

The service GC processes all known references for a service and marks the service as a candidate for eager GC
  if _all_ references used to access the service are stale.
An individual reference, i.e. the `reference-type->entry` map computed in a request descriptor,
  is stale if any of its value entries is stale.
This staleness check on the value is performed using the functions returned from
  `retrieve-reference-type->stale-fn` of the builder and invoking the corresponding 'type' function on the value.

**Note**: a service that has an empty map among its references never goes stale as it is known to be directly referenced.

The default implementation of the `ServiceDescriptionBuilder` returns a map with a single entry for `:token`
  from the `retrieve-reference-type->stale-fn`.
The provided `:token` staleness function deems services accessed via tokens to be stale if all tokens
  used to access the service have been updated.

Custom builder implementations can add additional reference types to services and
  need to provide appropriate staleness check functions for each reference type.
E.g. a hypothetical implementation which treats the image parameter as docker images can
  have the following entry for a service:
```
  {:image {:name "twosigma/kitchen" :build "20191001"}
   :token {:sources [{"token" "foo" "version" "v1"}]}}
```
The `retrieve-reference-type->stale-fn` must then provide an implementation for a function that
  can check staleness of `:image` reference types.
