import React, { Component } from 'react';
import { Route } from 'react-router-dom';

class TabRoute extends Component {
  constructor(props) {
    super(props);
  }

  componentWillMount() {
    console.log(`Going to '${this.props.path}'`);
    this.props.onMount();
  }

  render() {
    return (
      <Route
        path={this.props.path}
        component={null}
      />
    );
  }
};

export default TabRoute;
