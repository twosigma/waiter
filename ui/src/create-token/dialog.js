import React, { Component } from 'react';
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  InputLabel,
  Switch,
  Toolbar,
  Typography,
  withStyles,
} from 'material-ui';
import SwipeableViews from 'react-swipeable-views';

import Form from './form';
import JsonEditor from './jsonEditor';

class TokenDialog extends Component {
  constructor(props) {
    super(props);
    this.state = {
      inJsonMode: false,
    };
  }

  close = () => {
    this.setState({ inJsonMode: false });
    this.props.close();
  }

  submit = () => {
    this.props.submit();
    this.close();
  }

  render() {
    const { classes, children, open } = this.props;
    const { inJsonMode } = this.state;
    return (
      <Dialog
        open={open}
        classes={{
          paper: classes.paper,
        }}
      >
        <Toolbar>
          <Typography type="title">
            Create Token
          </Typography>
          <InputLabel
            shrink
            className={classes.jsonLabel}
          >
            JSON Mode
          </InputLabel>
          <Switch
            checked={inJsonMode}
            onChange={(event, inJsonMode) => {
              this.setState({ inJsonMode });
            }}
            className={classes.jsonSwitch}
          />
        </Toolbar>
        <DialogContent
          className={classes.dialogContentContainer}
        >
          <SwipeableViews
            index={inJsonMode ? 1 : 0}
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
            <Form className={classes.dialogContent} />
            <JsonEditor className={classes.dialogContent} />
          </SwipeableViews>
        </DialogContent>
        <DialogActions>
          <Button onClick={this.close}>
            Cancel
          </Button>
          <Button
            raised
            color="primary"
            onClick={this.submit}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    );
  }
};

export default withStyles({
  paper: {
    width: 740,
    minWidth: 530,
    height: '80%',
    maxHeight: 800,
  },
  jsonLabel: {
    marginRight: -20,
    position: 'absolute',
    right: 60,
  },
  jsonSwitch: {
    position: 'absolute',
    right: 0,
  },
  dialogContentContainer: {
    padding: '0 !important',
    height: 'calc(100% - 100px)',
    minHeight: 'calc(100% - 120px)',
  },
  dialogContent: {
    height: '100%',
    minHeight: '100%',
    width: '100%',
  }
})(TokenDialog);
