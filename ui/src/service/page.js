import React, { Component } from 'react';
import { Icon, Paper, Toolbar, withStyles } from 'material-ui';

import ServiceDescription from './description';
import InstanceTable  from './instanceTable';

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
        <ServiceDescription
          id={id}
        />
      </Paper>
      <Paper classes={{
        root: classes.instanceTableContainer,
      }}>
        <InstanceTable
          id={id}
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
