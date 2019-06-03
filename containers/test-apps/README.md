This folder contains apps (services) designed for exercising test scenarios in Waiter.
It also includes a script to create the docker image for use in the different schedulers.
The docker image includes executables to the different test apps.

The following test apps are included:
- courier:
  A simple gRPC server written in Java that responds to http/2 cleartext requests.
  An additional web server is launched that can be used to respond to health checks.
  It is mainly used to test gRPC support.
- kitchen:
  A simple app written in python that responds to http/1.1 and websocket requests.
  It enables testing websockets and support for other Waiter parameters.
  It also tests Waiter behavior for various corner cases, like aborting requests, streaming data, etc.
- nginx:
  An nginx server that responds to cleartext and secure http/1.1 and http/2 requests. 
  It is mainly used to test http/2 backend-proto support.
- sediment:  
  An jetty server that responds to cleartext http/1.1 and http/2 requests. 
  It is mainly used to test trailers support.
