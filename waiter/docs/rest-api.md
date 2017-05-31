This API description is meant to be an overview of methods available in the REST API.

|Path                |Verb    |Description|
|--------------------|--------|-----------|
|`/apps`             |`GET`   |List all services that are currently running in Waiter. Waiter starts services when a request is received. It shuts them down after an idle timeout.|
|`/apps/<service-id>`|`GET`   |Get detailed information for a particular service, including the service description, healthy and unhealthy instances, and metrics.|
|`/apps/<service-id>`|`DELETE`|Kill all instances of a service. Note that outstanding/future requests may cause Waiter to restart instances of the service.|
|`/service-id`       |`GET`   |Get the service-id for a given application. The service-id will be looked up using a combination of the headers/tokens provided in the request.|
|`/status`           |`GET`   |Check on the status of Waiter.|
|`/token`            |`GET`   |Retrieve information about a token. Token should be passed in the `X-Waiter-Token` header.|
|`/token`            |`POST`  |Define a token. Tokens can store all or part of the service description.|
|`/token`            |`DELETE`|Remove a token. Token should be passed in the `X-Waiter-Token` header.|
