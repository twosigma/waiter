The sediment app is an HTTP server designed specifically for exercising http/1.1 and http/2 trailer support test scenarios in Waiter.
The server can be started by using the [run-sediment-server.sh](bin/run-sediment-server.sh) script in the bin directory.

# Building the app:
Before we can run the app, it must be built using the following command:
  `mvn clean package`

Running the above command creates a `sediment-uberjar.jar` in the `data` directory.
