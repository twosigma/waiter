# Service References

To process an HTTP request, Waiter needs to resolve a request to a service description.
This service description tells Waiter how to locate the service instance to use to process the current request.

Waiter uses request headers to construct the service description using the `ServiceDescriptionBuilder`.
It is possible for the _same_ service to be reachable from various combinations of headers:
  1. token - single or multiple
  1. on-the-fly services without tokens
  1. on-the-fly services that extend tokens, e.g. run-as-requester

## Service GC

By default, Waiter relies on the service idle timeout to GC services after periods of inactivity (not receiving requests).
However, services reachable only via references, e.g. tokens, can be GC-ed eagerly if the reference has been updated.

**Note**: A service known to be referenced only via on-the-fly headers never goes stale.

When a service description is constructed from a request, the set of references for a service are also updated.
Individual references are available in the `:reference-type->entry` key in the descriptor,
  the descriptor being built per request.
It is possible to reference the same service using different combinations of on-the-fly headers and tokens.
As such, there is a set of references (the union of all `:reference-type->entry` from the descriptor) that
  can be used to reference a service.

### Reference (`:reference-type->entry` in the descriptor) examples

The `reference-type->entry` is a map where the keys represent a `:type` parameter used during staleness checks.

- A service created with on-the-fly header without tokens never goes stale.
  It has an empty map as the value for `:reference-type->entry` in the descriptor:
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

As mentioned previously, as the same service can be accessed by another token, we end up
  building a set of `reference-type->entry` maps as references that refer to the service.
E.g. if all the example requests above mapped to the same service, that service would have
  the following references set:
```
  #{{}
    {:token {:sources [{:token "foo" :version "v0"}]}}
    {:token {:sources [{:token "bar" :version "v1"}
                       {:token "baz" :version "v2"}]}}}
```

## Staleness checks

The service GC processes all known references for a service and marks the service as a candidate for eager GC
  if _all_ references used to access the service are stale.
An individual reference, i.e. the `reference-type->entry` map computed in a request descriptor,
  is stale if any of its value entries is stale.
This staleness check on the value is performed using the functions returned from
  `retrieve-reference-type->stale-fn` of the builder and invoking the corresponding 'type' function on the value.

**Note**: A service known to be directly referenced (i.e. has an empty map among its references) never goes stale.

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
E.g. if `retrieve-reference-type->stale-fn` returns:
```
  {:image check-image-for-staleness-fn
   :token check-token-for-staleness-fn}
```
and we have a service with the following set of references:
```
  #{{:image {:name "twosigma/courier" :build "20191002"}}
    {:image {:name "twosigma/kitchen" :build "20191001"}
     :token {:sources [{:token "foo" :version "v0"}]}}
    {:token {:sources [{:token "bar" :version "v1"}
                       {:token "baz" :version "v2"}]}}}
```
then the service goes stale when the following condition is true:
```
  (and
    # single image expression in the or since there was only one entry in the map
    (or (check-image-for-staleness-fn {:name "twosigma/courier" :build "20191002"}))
    # two expressions in the or, one for image and one for token
    (or (check-image-for-staleness-fn {:name "twosigma/kitchen" :build "20191001"})
        (check-token-for-staleness-fn {:sources [{:token "foo" :version "v0"}]}))
    # single token expression in the or since there was only one entry in the map
    (or (check-token-for-staleness-fn
          {:sources [{:token "bar" :version "v1"} {:token "baz" :version "v2"}]})))
```
