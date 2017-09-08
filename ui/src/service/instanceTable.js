import React, { Component } from 'react';
import { Toolbar, Tab, Tabs } from 'material-ui';
import SwipeableViews from 'react-swipeable-views';
import ServiceDescription from './description';

class InstanceTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tabIndex: 0,
    };
  }

  render() {
    const { tabIndex } = this.state;
    const { description, id } = this.props;
    return (
      <div>
        <Toolbar disableGutters>
          <Tabs
            indicatorColor="primary"
            textColor="primary"
            value={tabIndex}
            onChange={(event, index) => {
              this.setState({ tabIndex: index });
            }}
          >
            <Tab label="Description" />
            <Tab label="Instances" />
            <Tab label="Failed Instances" />
            <Tab label="Killed Instances" />
            <Tab label="Router Metrics" />
          </Tabs>
        </Toolbar>
        <SwipeableViews
          index={tabIndex}
          style={{
              width: '100%',
              height: '100%',
              minHeight: '100%',
            }}
          containerStyle={{
              width: '100%',
              height: '100%',
              minHeight: '100%',
            }}
        >
          <ServiceDescription
            id={id}
            description={description}
          />
          <div>Instances</div>
          <div>Failed Instances</div>
          <div>Killed Instances</div>
          <div>Router Metrics</div>
        </SwipeableViews>
      </div>
    );
  }
};

export default InstanceTable;
