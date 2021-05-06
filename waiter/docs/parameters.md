Waiter's behavior is customized through parameters specified by the HTTP client.


### Table of Contents
[Basic Parameters](#basicparameters)  
[Optional Parameters](#optionalparameters)  
[Token Parameters](#tokenparameters)  


### Basic Parameters
<a name="basicparameters"/>
The basic parameters that Waiter supports are:

|Parameter|Required?|Default Value|Description|
|---------|---------|-------------|-----------|
|`x-waiter-cmd`|**Yes**|n/a|The shell command to run. Your application must listen on a port chosen by Waiter. The port your service should listen in is set via the `$PORT0` environment variable.|
|`x-waiter-cpus`|**Yes**|n/a|The number of cores your service needs.|
|`x-waiter-health-check-url`|No|"/status"|Specifies an HTTP endpoint that Waiter will hit to determine if your app is healthy. This should return `200 OK`.|
|`x-waiter-mem`|**Yes**|n/a|The amount of memory your service needs (MB). You will still need to explicitly include any memory flags (e.g. `-Xmx`) in your command.|
|`x-waiter-name`|No|empty string|A name for your service. Use only letters, numbers, dashes, underscores, and dots.|
|`x-waiter-run-as-user`|No|user who is making the request|The user that the service should be run as.|
|`x-waiter-version`|**Yes**|n/a|Specify a version to associate with your service (e.g. "a0b1c2d3e4f5" or "my-version-name").|


### Optional Parameters
<a name="optionalparameters"/>
Additional (optional) parameters that can be set:

|Parameter|Default Value|Valid Values|Description|Guidance|
|---------|-------------|------------|-----------|--------|
|`x-waiter-allowed-params`|empty|array of non-empty strings (token) or comma-separated strings (headers)|The environment variables configured as parameters for a tokenized service via `x-waiter-param-*` header which will not trigger an on-the-fly mode.|Expose only environment variables which your command can handle safely in this list. You can also choose to provide default values for these parameters in your `env` configuration.|
|`x-waiter-authentication`|standard|disabled, standard or \<authentication provider\>|The authentication mechanism to use for incoming requests.|By default, Waiter authenticates incoming requests using the standard protocol (e.g. Kerberos). If you would prefer that Waiter not authenticate incoming requests, set this flag to disabled. You can also specify the specific authentication provider if supported by the configured authenticator.|
|`x-waiter-backend-proto`|http|http, https, h2 or h2c|The backend connection protocol to use.|By default, Waiter connects to backend instances using the HTTP protocol. If you would prefer that Waiter use HTTPS, feel free to set this parameter to https. Both http and https protocol connect to the backend using http/1.1. Waiter, currently (due to a [limitation in the Jetty client](https://github.com/eclipse/jetty.project/issues/1350)), does not automatically upgrade https requests to HTTP/2. The h2 and h2c protocols can be used to specify using http/2 with prior knowledge while connecting to backends.|
|`x-waiter-cmd-type`|"shell"|"shell"|Provides an extension point for supporting different types of commands in the future.|Feel free to add new command types to suit your needs.|
|`x-waiter-concurrency-level`|1|1-10000|The number of simultaneous requests to an individual backend instance.|Increasing this value will likely lead to better performance and better resource utilization. Avoid queuing your requests on your backend instances by not increasing concurrency level above what an individual instance can handle. The shorter your requests, the more benefit you will get out of a higher concurrency level.|
|`x-waiter-distribution-scheme`|"balanced"|"balanced" or "simple"|The strategy used by Waiter routers to distribute instances amongst themselves. A "simple" distribution, achieved using consistent hashing, will minimize instances being assigned to multiple routers due to scaling. A "balanced" distribution ensures even splitting of instances across routers after the initial consistent hash distribution.|If you see too many ejected instances due to 503 responses, it may be a good idea to try out "simple" distribution.|
|`x-waiter-env-*`|n/a|any string|An environment field. Environment fields allow you to pass arbitrary string value to the environment executing your command. The variable name is formed by trimming the `x-waiter-env-` prefix and then uppercasing, e.g. `x-waiter-env-foo_bar2` is converted to the variable `FOO_BAR2`.|Use as needed.|
|`x-waiter-expired-instance-restart-rate`|0.1|(0-1]|The max percentage of total expired instances to recycle at the same time (e.g. if the service has 10 expired instances and a `x-waiter-expired-instance-restart-rate` of 0.1, Waiter will only restart 1 at a time).|The default is reasonable for most cases.|
|`x-waiter-fallback-period-secs`|300|[0-86400]|The amount of time, in seconds, to allow fallback to a previous version of the service since the token update. Requests will always be forwarded to the latest version of a service if it has a healthy instance running.|Don't change it unless you have a good reason to do so. Setting it to zero will disable fallback support, i.e. requests will always be forwarded to the latest version of a service whether or not it has a healthy instance running, which may result in requests blocking.|
|`x-waiter-grace-period-secs`|30|0-3600 (1 hour)|The amount of time to wait after a service starts before considering a health check failure to be an error. A value of zero means disable restarting the instances even if it is constantly reporting unhealthy.|Set this value to startup time of your service. If your service starts up within 30 seconds, don't bother with changing this value.|
|`x-waiter-health-check-authentication`|disabled|disabled or standard|The authentication mechanism to use for health check requests.|By default, Waiter uses no authentication for health check requests. Health check requests can be asked to use the standard protocol (i.e. Basic auth).|
|`x-waiter-health-check-interval-secs`|10|5-60 (1 minute)|The amount of time to wait between health checks.|Don't change it unless you have a good reason to do so.|
|`x-waiter-health-check-max-consecutive-failures`|5|1-15|Number of consecutive health check failures after which the unhealthy instance will be terminated.|Don't change it unless you have a good reason to do so.|
|`x-waiter-health-check-port-index`|0|0-9|The index in this service's ports to be use for health check requests using the HTTP/1.1 protocol.|Don't change it unless your service cannot respond to HTTP/1.1 health checks on $PORT0.|
|`x-waiter-health-check-proto`|<same as backend-proto>|http, https, h2 or h2c|The backend connection protocol to use for health checks.|By default, Waiter connects to backend instances for health checks using the same protocol as the backend-proto. If you would prefer that Waiter use a different protocol (described in the backend-proto section), you can explicitly set this parameter.|
|`x-waiter-https-redirect`|false|true or false|When set to `true`, Waiter requires clients to use the HTTPS secure communication protocol while making requests to the service. By default, Waiter allows non-secure HTTP requests from clients to services.|Set it to `true` if you are sensitive about the integrity of exchanged data between the client and your service instances.|
|`x-waiter-idle-timeout-mins`|4320 (3 days)|0-43200 (30 days)|The number of minutes of no activity to wait before shutting down a service completely.|Setting a lower idle timeout is strongly encouraged and frees cluster resources for other services to use. A value of zero disables shutting down the service.|
|`x-waiter-image`|empty|Docker image string|Specify the image to use for the application, otherwise the default-container-image is used. Not all schedulers support this functionality.|Set this value when you want a custom image for your service.|
|`x-waiter-instance-expiry-mins`|7200 (5 days)|>=0|The amount of time to allow a particular instance of your app to run before restarting it. Waiter ensures another instance is available before killing the expiring instance such that service is unaffected. A value of zero means instances never expire.|You can set this value to be as large as you like.|
|`x-waiter-interstitial-secs`|0|1-3600 (1 hour)|The amount of time after a service is started that the interstitial page will be displayed. A value of zero means the interstitial page will never be displayed.|Set this value to startup time of your service. If your service starts up within a few seconds, don't bother with changing this value.|
|`x-waiter-jitter-threshold`|0.5|[0,1)|How big the difference between ideal and current number of instances must be for Waiter to scale your service.|The default is reasonable for most cases.|
|`x-waiter-load-balancing`|"oldest"|"oldest", "youngest" or "random"|The load balancing scheme used while allocating instances to service a request.|By default, Waiter routes requests to the oldest available instance. If you would prefer that Waiter send requests to youngest or random idle backends, set this flag to "youngest" or "random", respectively.|
|`x-waiter-max-instances`|500|1-500|The maximum number of instances that Waiter should not scale above.|Setting a max can be helpful for preventing unexpected scaling.|
|`x-waiter-max-queue-length`|1000|>0|The number of requests to queue before rejecting requests with a 503 error code.|Lowering this value to something reasonable for your architecture is encouraged as it allows you to fail faster. Don't increase the value without a good reason.|
|`x-waiter-metadata-*`|n/a|any string|A metadata field. Metadata fields allow you to store arbitrary strings along with your service description.|Allows additional information to be stored with a service to help identify the service.|
|`x-waiter-metric-group`|"other"|[A-Za-z0-9-_]+|Metric groups allow services to be grouped together for the purpose of metric collection.|If you have `:statsd` enabled in your [config file](../config-full.edn), then specify a metric group. If you have multiple services, choose the same metric group if you'd like combined metrics, otherwise choose independent metric groups.|
|`x-waiter-min-instances`|1|1-4|The minimum number of instances that Waiter should keep running at all times.|Go with the default.|
|`x-waiter-namespace`|empty|user-name|The namespace in which to create back-end scheduler objects. When set, must match the run-as-user value.|Leave this field blank unless you need direct access to resources Waiter creates on your behalf.|
|`x-waiter-param-*`|n/a|any string|A parameter field. Parameter fields allow you to pass arbitrary string value to the environment executing your command. The variable name is formed by trimming the `x-waiter-param-` prefix and then uppercasing, e.g. `x-waiter-param-foo_bar2` is converted to the variable `FOO_BAR2`. Any passed param (e.g. `FOO_BAR2`) must be present in the `Allowed-Params` parameter value of the service.|Use as needed.|
|`x-waiter-permitted-user`|user who is making the request|any user|The user that is authorized to make requests to your service|Use as needed.|
|`x-waiter-ports`|1|[1-10]|The number of ports needed by the service.|Use as needed, only restriction is that the first port ($PORT0) must always be used for a web server responding to incoming web requests using the specified (configured by backend-proto) HTTP/1.1 or HTTP/2 protocol.|
|`x-waiter-queue-timeout`|300000 (5 mins)|>0|The maximum time, in milliesconds, allowed spent by a request waiting for an available service backend.|Don't change it unless you have a good reason to do so.|
|`x-waiter-restart-backoff-factor`|2|>=1|The factor by which to multiple the amount of time between consecutive start-up failures. The base is 1 second.|The default is reasonable for most cases.|
|`x-waiter-scale-down-factor`|0.1|(0-1]|The percentage amount, per second, to decrease the current number of instances toward the target number of instances. See [How Waiter Autoscaling Works](autoscaling.md).|Don't change it unless you have a good reason to do so.|
|`x-waiter-scale-factor`|1|(0-2]|The ratio of desired number of backends to number of outstanding requests. See [How Waiter Autoscaling Works](autoscaling.md).|Don't change it unless you have a good reason to do so, although values lower than 1 promote more efficient usage of resources if you don't mind waiting.|
|`x-waiter-scale-up-factor`|0.01|(0-1]|The percentage amount, per second, to increase the current number of instances toward the target number of instances. See [How Waiter Autoscaling Works](autoscaling.md).|Don't change it unless you have a good reason to do so.|
|`x-waiter-scheduler`|empty|<a supported scheduler id in the composite scheduler>|The id of the scheduler to use while creating this service. For composite schedulers, when provided, it must match the name of the component scheduler.|Don't configure it unless you are using composite scheduler and you know which specific component scheduler you want to use as default.|
|`x-waiter-service-mapping`|"legacy"|"legacy" or "exclusive"|Determines whether tokens having the same configuration will map to the same service (legacy mode) or to different services (exclusive mode).|Don't change unless your service needs access to the token (dns) name.|
|`x-waiter-stale-timeout-mins`|15|[0-240]|Represents the amount of time, in minutes, before stale services are GC-ed. Stale services will have their idle timeout reduced to stale-timeout-mins plus fallback-period-secs.|Don't change it unless you have a good reason to do so. Setting it and fallback-period-secs to zero will cause stale services to be GC-ed most eagerly.|
|`x-waiter-termination-grace-period-secs`|0|0-300 (5 minutes)|The amount of time (in seconds) between the executor sending SIGTERM to a task and then sending SIGKILL to enable graceful termination. A value of zero translates to using the default value provided by the underlying scheduler.|If your service can shutdown promptly within a handful of seconds, don't bother with changing this value.|
|`x-waiter-timeout`|900000 (15 mins)|>0|The socket timeout, in milliesconds, of the connection between Waiter and a service backend.|Don't change it unless you have a good reason to do so.|


### Token Parameters
<a name="tokenparameters"/>
Token Parameters are optional parameters that can only be set on the token, and not passed in a header:

|Parameter|Default Value|Valid Values|Description|Guidance|
|---------|-------------|------------|-----------|--------|
|`cors-rules`|empty|array of CORS rule objects|List of rules to allow CORS. CORS is allowed if a request matches any of the rules.<br>A rule is a JSON object with 3 fields:<br>`origin-regex`: Required. A valid regular expression that must match the entire origin (origin starts with scheme e.g. "https://")<br>`target-path-regex`: Optional. A valid regular expression that must match the entire path (path starts with "/")<br>`methods`: Optional. A list of uppercase HTTP methods to allow (all methods allowed if this field is not set)||
