import React, { Component } from 'react';
import { Icon, Paper, Toolbar, withStyles } from 'material-ui';

import ServiceDescription from './description';
import InstanceTable  from './instanceTable';

const description = {
    "metric-group": "waiter_kitchen",
    "name": "testheadermetadatadsm5754536",
    "metadata": {
        "begindate": "null",
        "timestamp": "20160713201333949",
        "foo": "bar",
        "enddate": "null",
        "baz": "quux"
    },
    "run-as-user": "dsm5",
    "idle-timeout-mins": 10,
    "version": "version-does-not-matter",
    "cmd-type": "shell",
    "mem": 256,
    "cmd": "/Users/dsm5/git/waiter/kitchen/bin/run.sh -p $PORT0",
    "health-check-url": "/status",
    "cpus": 0.1,
    "permitted-user": "dsm5"
};

const Service = ({ id, classes }) => (
  <div>
    <Toolbar classes={{
      root: classes.toolbar,
    }}>
      clusterA
      <Icon classes={{
        root: classes.chevronIcon,
      }}>
        chevron_right
      </Icon>
      waiter-service-id
    </Toolbar>
    <div
      className={classes.container}
    >
      <Paper classes={{
        root: classes.descriptionContainer,
      }}>
      <div />
      </Paper>
      <Paper classes={{
        root: classes.instanceTableContainer,
      }}>
        <InstanceTable
          id={id}
          description={description}
        />
      </Paper>
    </div>
  </div>
);

export default withStyles((theme) => ({
  toolbar: {
    position: 'fixed',
    width: '100%',
    top: 0,
  },
  chevronIcon: {
    fontSize: '1.2em !important',
    color: theme.palette.primary[800],
    margin: '0px 7px',
  },
  container: {
    margin: '0 20px',
    marginTop: 64,
  },
  descriptionContainer: {
    marginBottom: 20,
    padding: 10,
  },
  instanceTableContainer: {
    padding: 10,
    paddingTop: 0,
  },
}))(Service);
