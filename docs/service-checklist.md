Ensure your service will work in Waiter by making sure you've taken care of the items on this page.

# Resources

Make sure you allocate enough memory and cpu for your service to run. If you don't, your service may not start.

# HTTP Port

The TCP port that your HTTP service listens on must be configurable from the command line. For example:

```
myservice/bin/run-my-service -p $PORT0
```

Take care to escape variables that start with `$` such that they are not interpreted by your local bash shell.

# Home Directory/Storage

Don't rely on persistent local storage like home directories. If your service needs temporary local storage, use `$MESOS_DIRECTORY`. Assume that anything you write to this directory will be deleted immediately when your service shuts down.
