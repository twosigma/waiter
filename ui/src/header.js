import React, { Component } from 'react';
import { withRouter } from 'react-router-dom';
import { connect } from 'react-redux';
import {
  IconButton,
  Tab,
  Tabs,
  Toolbar,
  Typography,
  withStyles,
} from 'material-ui';
import { LightbulbOutline as LightbulbOutlineIcon } from 'material-ui-icons';

import TokenButton from './create-token/button';
import { toggleTheme } from './actions';

const WaiterLogo = () => (
  <Typography
    style={{
      height: '48px',
      lineHeight: '48px',
      minWidth: '180px',
      margin: '10px',
      fontFamily: '"Nothing You Could Do", cursive',
      textAlign: 'center',
      verticalAlign: 'middle',
      fontSize: '30px',
    }}
  >
    Waiter
  </Typography>
);

const Header = ({ classes, tabs, tabIndex, history, onTabChange, onLightClick }) => (
  <Toolbar
    disableGutters
    className={classes.appbar}
  >
    <WaiterLogo />
    <Tabs
      indicatorColor="primary"
      textColor="primary"
      value={tabIndex}
      onChange={(event, tabIndex) => {
        onTabChange(tabIndex);
        history.push(`/${tabs[tabIndex]}`);
      }}
    >
      {
        tabs.map((tab) => (
          <Tab key={tab} label={tab} />
        ))
      }
    </Tabs>
    <div className={classes.spacer} />
    <IconButton
      title="Toggle theme"
      onClick={onLightClick}
    >
      <LightbulbOutlineIcon />
    </IconButton>
    <TokenButton
      classes={{
        button: classes.tokenButton,
      }}
    />
  </Toolbar>
);

const styledHeader = withStyles((theme) => ({
  appbar: {
    backgroundColor: (
      (theme.palette.type === 'light')
        ? theme.palette.background.paper
    : theme.palette.background.appBar
    ),
    zIndex: 1300,
    boxShadow: theme.shadows[2],
  },
  spacer: {
    flex: 1,
  },
  tokenButton: {
    marginRight: 10,
  },
}))(withRouter(Header));

export default connect(
  () => ({}),
  (dispatch) => ({
    onLightClick: () => dispatch(toggleTheme()),
  })
)(styledHeader);
