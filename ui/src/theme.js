import React, { Component } from 'react';
import { connect } from 'react-redux';
import { MuiThemeProvider, colors, createMuiTheme } from 'material-ui';

const getTheme = (theme) => (
  createMuiTheme({
    palette: {
      type: theme,
      primary: colors.cyan,
      secondary: colors.amber,
      error: colors.red,
    },
    overrides: {
      MuiButton: {
        raisedPrimary: {
          color: 'white',
        },
        flatPrimary: {
          color: 'white',
        },
      },
    },
  })
);

class Theme extends Component {
  constructor(props) {
    super(props);
    this.state = {
      theme: getTheme(this.props.theme),
    };
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.theme !== nextProps.theme) {
      this.setState({
        theme: getTheme(nextProps.theme),
      });
    }
  }

  render() {
    return (
      <MuiThemeProvider theme={this.state.theme}>
        {this.props.children}
      </MuiThemeProvider>
    );
  }
};

export default connect((state) => ({
  theme: state.getIn(['settings', 'theme']),
}))(Theme);
