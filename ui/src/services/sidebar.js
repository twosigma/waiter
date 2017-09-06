import React, { Component } from 'react';
import {
  Checkbox,
  List,
  ListItem,
  ListItemIcon,
  ListItemSecondaryAction,
  ListItemText,
  TextField,
  colors,
  withStyles,
} from 'material-ui';

import { DropdownTextField } from '../common';

const Sidebar = ({
  user,
  userOptions,
  metricGroup,
  metricGroupOptions,
  clusters,
  onClusterClick,
  classes,
}) => (
  <div>
    <List className={classes.sidebar}>
      <ListItem>
        <DropdownTextField
          label="Username"
          value={user}
          options={userOptions}
          classes={{
            suggestionsContainerOpen: classes.dropdownTextField,
          }}
        />
      </ListItem>
      <ListItem>
        <DropdownTextField
          label="Metric Group"
          value={metricGroup}
          options={metricGroupOptions}
          classes={{
            suggestionsContainerOpen: classes.dropdownTextField,
          }}
        />
      </ListItem>
      {
        Object.keys(clusters).map((cluster) => (
          <ListItem key={cluster}>
            <ListItemText primary={cluster} />
            <ListItemSecondaryAction>
              <Checkbox
                checked={clusters[cluster]}
                onChange={() => onClusterClick(cluster)}
              />
            </ListItemSecondaryAction>
          </ListItem>
        ))
      }
    </List>
  </div>
);

export default withStyles((theme) => ({
  dropdownTextField: {
    maxHeight: 310,
    zIndex: 1500,
  },
  sidebar: {
    height: 'calc(100vh - 85px)',
    width: 200,
    minWidth: 200,
    float: 'left',
    backgroundColor: theme.palette.background.default,
  },
}))(Sidebar);
