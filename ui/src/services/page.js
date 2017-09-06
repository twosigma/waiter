import React, { Component } from 'react';
import { withStyles } from 'material-ui';
import SwipeableViews from 'react-swipeable-views';

import { TabRoute } from '../common';

import Services from './services';
import Service from './service';

class Page extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    const parts = window.location.pathname.slice(1).split('/');
    const cluster = parts[1];
    const serviceID = parts[2];

    if (serviceID) {
      this.lastGoodServiceID = serviceID;
    }

    return (
      <div>
        <SwipeableViews
          index={serviceID ? 1 : 0}
        >
          <Services />
          <Service id={this.lastGoodServiceID} />
        </SwipeableViews>
      </div>
    );
  }
};
Page.lastGoodServiceID = undefined;

export default withStyles((theme) => ({
}))(Page);
