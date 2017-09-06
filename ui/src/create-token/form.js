import React, { Component } from 'react';
import {
  List,
  ListItem,
  ListItemText,
  withStyles
} from 'material-ui';

const Indicator = ({ tabIndex, classes }) => (
  <div>
    <div
      className={classes.indicator}
      style={{
        top: (48 * tabIndex),
      }}
    />
    <div className={classes.bar} />
  </div>
);

class Form extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tabIndex: 0,
    };
  }

  render() {
    const { className, classes } = this.props;
    return (
      <div
        style={{
          position: 'relative',
        }}
        className={className}
      >
        <List
          disablePadding
          className={classes.list}
        >
          {
            ['A', 'B', 'C', 'D'].map((label, tabIndex) => (
              <ListItem
                key={label}
                button
                onClick={() => this.setState({ tabIndex })}
              >
                <ListItemText primary={label} />
              </ListItem>
            ))
          }
        </List>
        <Indicator
          tabIndex={this.state.tabIndex}
          classes={{
            indicator: classes.indicator,
            bar: classes.indicatorBar,
          }}
        />
      </div>
    );
  }
};

export default withStyles((theme) => ({
  list: {
    width: 150,
  },
  indicator: {
    position: 'absolute',
    zIndex: 2,
    left: 150,
    height: 48,
    width: 2,
    backgroundColor: theme.palette.primary[500],
    transition: 'all 300ms cubic-bezier(0.4, 0, 0.2, 1) 0ms',
  },
  indicatorBar: {
    position: 'absolute',
    zIndex: 1,
    top: 0,
    left: 150,
    height: '100%',
    width: 2,
    backgroundColor: theme.palette.grey[200],
  },
}))(Form);
