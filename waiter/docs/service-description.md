# Service Description Support in Waiter

Waiter is an autoscaling microservices platform, built to scale applications (service instances) running on [Mesos](http://mesos.apache.org/).
Waiter enables registration of _service descriptions_ to remove the need for the user to ever explicitly deploy service-based applications.
Waiter uses the service description to create and destroy _service instances_ on-demand.
In many ways Waiter can be considered as a dynamic load balancer which has the additional capability to launch an instance to process a request if one doesn't already exist.

## Creating/Editing a Service Description

To process a http request, Waiter needs to resolve a request to a service description.
This service description tells Waiter how to locate the service instance to use to process the current request.
As mentioned above, Waiter will launch an instance of your service if it doesn't already exist before forwarding the request.
