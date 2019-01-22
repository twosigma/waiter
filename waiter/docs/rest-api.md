This API description is meant to be an overview of methods available in the REST API.

|Path                |Verb    |Description|
|--------------------|--------|-----------|
|`/apps`             |`GET`   |List all services that are currently running in Waiter. Waiter starts services when a request is received. It shuts them down after an idle timeout.|
|`/apps/<service-id>`|`GET`   |Get detailed information for a particular service, including the service description, healthy and unhealthy instances, and metrics.|
|`/apps/<service-id>`|`DELETE`|Kill all instances of a service. Note that outstanding/future requests may cause Waiter to restart instances of the service.|
|`/service-id`       |`GET`   |Get the service-id for a given application. The service-id will be looked up using a combination of the headers/tokens provided in the request.|
|`/status`           |`GET`   |Check on the status of Waiter.|
|`/token`            |`GET`   |Retrieve information about a token. Token should be passed in the `X-Waiter-Token` header. An extra `include=metadata` query parameter can be used to retrieve metadata about the token. An extra `include=deleted` query parameter can be used to retrieve a soft-deleted token.|
|`/token`            |`POST`  |Define a token. Tokens can store all or part of the service description. An extra `update-mode=admin` query parameter can be used to trigger a forced update of a token by a user with admin privileges.|
|`/token`            |`DELETE`|Remove a token. Token should be passed in the `X-Waiter-Token` header. Tokens can either be soft-deleted or hard-deleted. If a token is soft-deleted, it can still be retrieved by `include=deleted` flag in the GET request. Hard-deleted tokens are permanently removed from the underlying store. An extra `hard-delete=true` query parameter can be used to trigger a hard-delete of the token.|
|`/tokens`           |`GET`   |Retrieve information about tokens. An extra `owner=<name>` query parameter can be used to tokens for a particular owner. An extra `include=deleted` query parameter can be used to retrieve a soft-deleted tokens. An extra `include=metadata` query parameter can be used to retrieve metadata about the tokens. |
