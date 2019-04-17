The courier app is a gRPC server designed specifically for exercising gRPC test scenarios in Waiter.
The server can be started by using the [run-courier-server.sh](bin/run-courier-server.sh) script in the bin directory.

# Building the app:
Before we can run the app, it must be built using the following command:
  `mvn clean package`

Running the above command creates a `courier-uberjar.jar` in the `data` directory.

### Historical tidbit

One of the first business uses of RPC was by Xerox under the name _Courier_ in 1981.