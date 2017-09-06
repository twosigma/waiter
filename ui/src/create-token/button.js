import React, { Component } from 'react';
import { Button, withStyles } from 'material-ui';
import { Add as AddIcon } from 'material-ui-icons';

import TokenDialog from './dialog'

class TokenPopupButton  extends Component {
  constructor(props) {
    super(props);
    this.state = {
      open: false,
    };
  }

  render() {
    return (
      <div>
        <Button
          raised
          color="primary"
          onClick={() => this.setState({ open: true })}
          className={this.props.classes.button}
        >
          <AddIcon />{'\u00a0'}Create Token
        </Button>
        <TokenDialog
          open={this.state.open}
          close={() => this.setState({ open: false })}
          submit={() => {}}
        />
      </div>
    );
  }
};

export default withStyles({
  button: {},
})(TokenPopupButton);
