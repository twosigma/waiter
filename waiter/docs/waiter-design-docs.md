# Waiter Design

Waiter is seperated into four main components that communicate with each other:

* Router
* Service discovery 
* Reservation
* Autoscaler

## Router

The router is the simplest component, and the most user facing.
It accepts requests from clients, routers them to the correct backend and returns the response.
Below is the flow of data and control from the router's point of view:

1. Determines what service is being requested
1. Reserves a backend to process the request from the reservation system
1. Sends the request to the backend
1. Releases the backend to the reservation system
1. Returns the response

## Service discovery

The service discovery component is broken up into two submodules:

* External data collectors (Marathon, Router list from ZK)
* Service data aggregator

### External data collectors

The external data collectors maintain Waiter's view of the data source. 
Each collector will periodically query its data source and push the data onto the data aggregator.

### Service data aggregator

The data aggregator combines the data from Marathon and the router list from ZK to produce the all important mapping from app-id to list of instances available to this particular Waiter server.
That mapping is called `service-id->my-instance->slots` in the code. The `service-id->my-instance->slots` map is pushed to the reservation system.

## Reservation system

The reservation system comprises a process for handling the reservation of instances per app as well as a process to maintain the mapping from app to instance reservation system.
We call the per app reservation process an instance reserver and we call the process that maintains the mapping from app to instance reserver the reservation demultiplexer.

The demultiplexer serves a few functions:

* Receives data from the Service data aggregator and passes the available instance list to each instance reserver.
* Spins up and tears down instance reservers according to the `service-id->my-instance->slots` map.
* Responds to router requests for the channel to communicate with specific instance reservers.

Each instance reserver maintains the list of all instances available as well as the list of instances currently reserved. 
Using those two lists, it responds to router requests to reserve and release instances.

## Autoscaler

The autoscaler works in cycles in which it:

1. Generates the _ideal_ number of backends per app
1. Computes how large of step to take from the current number of backends toward the _ideal_
1. Scales up or down accordingly
