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
However, service reachable only via references, e.g. tokens, can be GC-ed eagerly if the reference has been updated.

When a service description is constructed from a request, the service references are also updated.
These references are available as the `:references` key in the descriptor and are annotated by a `:type` parameter.
The service GC process checks these references by type and marks a service as a candidate for eager GC
  if _all_ references used to access the service are stale.
This staleness check is performed using the functions returned from `retrieve-reference-type->stale-fn` of the builder.

The default implementation of the `ServiceDescriptionBuilder` returns
  the following functions for the different reference types:
  1. all services that can be directly accessed never go stale;
  1. services accessed via tokens go stale if all tokens used to access the service have been updated.

Custom builder implementations can add additional references to services and
  need to provide appropriate staleness check functions for each reference type.