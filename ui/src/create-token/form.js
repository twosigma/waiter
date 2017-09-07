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

const fields = [
  {
    tab: 'General',
    fields: [
      'Cluster',
      'Owner',
      'Token', 'Name', 'Version',
      'CPUs', 'Memory (MB)',
      'Command',
    ],
  },
  {
    tab: 'Permissions',
    fields: [
      'Run as user',
      'Permitted user(s)',
      'Metric group',
      'Authentication'
    ],
  },
  {
    tab: 'Health check',
    fields: [
      'Protocol',
      'Path',
      'Grace period (seconds)',
      'Idle timeout (minutes)',
      'Blacklist on 503'
    ],
  },
  {
    tab: 'Environment Variables',
    fields: [
    ],
  },
  {
    tab: 'Metadata',
    fields: [
    ],
  },
  {
    tab: 'Advanced',
    fields: [
      'Concurrency',
      'Distribution scheme',
      'Expired instance restart rate',
      'Instance expiration (minutes)',
      'Jitter threshold',
      'Minimum instances',
      'Maximum instances',
      'Maximum queue length',
      'Ports',
      'Restart backoff factor',
      'Scale factor',
      'Scale-down factor',
      'Scale-up factor',
    ],
  },
];

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
            fields.map(({ tab, fields }, tabIndex) => (
              <ListItem
                key={tab}
                button
                onClick={() => this.setState({ tabIndex })}
                classes={{
                  button: classes.onTabHover,
                }}
              >
                <ListItemText
                  primary={tab}
                  classes={{
                    root: classes.tabLabelRoot,
                    text: ((this.state.tabIndex === tabIndex) ?
                           classes.activeTabLabel :
                           classes.tabLabels),
                  }}
                />
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
    width: 170,
  },
  onTabHover: {
    '&:hover': {
      backgroundColor: 'transparent',
    },
  },
  tabLabelRoot: {
    padding: 0,
  },
  tabLabels: {
    fontSize: 13,
  },
  activeTabLabel: {
    fontSize: 13,
    color: theme.palette.primary[500],
  },
  indicator: {
    position: 'absolute',
    zIndex: 2,
    left: 170,
    height: 48,
    width: 2,
    backgroundColor: theme.palette.primary[500],
    transition: 'all 300ms cubic-bezier(0.4, 0, 0.2, 1) 0ms',
  },
  indicatorBar: {
    position: 'absolute',
    zIndex: 1,
    top: 0,
    left: 170,
    height: '100%',
    width: 2,
    backgroundColor: theme.palette.grey[200],
  },
}))(Form);
