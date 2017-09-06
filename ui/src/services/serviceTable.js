import React, { Component } from 'react';
import { withRouter } from 'react-router-dom';
import {
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TableSortLabel,
  Toolbar,
  Typography,
  withStyles,
  withTheme,
} from 'material-ui';
import { Delete as DeleteIcon } from 'material-ui-icons';
import SearchBar from 'material-ui-search-bar';
import Color from 'color';

class CustomSearchBar extends SearchBar {
  handleFocus = () => {
    this.setState({ focus: true });
    if (this.props.onFocus) {
      this.props.onFocus();
    }
  }
  handleBlur = () => {
    this.setState({ focus: false });
    if (this.props.onBlur) {
      this.props.onBlur();
    }
  }
}

class ServiceTableToolbar_ extends Component {
  constructor(props) {
    super(props);
    this.state = {
      focus: false,
    };
  }

  render() {
    let boxShadowColor = 'rgba(0, 0, 0, 0.5)';
    if (this.state.focus) {
      const { palette } = this.props.theme;
      boxShadowColor = (palette.type === 'light' ?
                        palette.primary['800'] :
                        palette.primary['A200']);
    }

    return (
      <Toolbar>
        <CustomSearchBar
          onFocus={() => this.setState({ focus: true })}
          onBlur={() => this.setState({ focus: false })}
          onChange={this.props.onSearch}
          onRequestSearch={() => {}}
          style={{
            width: 400,
            marginTop: 25,
            marginBottom: 15,
            boxShadow: (`inset 0px 1px 2px 0px  ${boxShadowColor},`
                      + 'inset 0px 1px 1px 0px  rgba(0, 0, 0, 0.14),'
                      + 'inset 0px 2px 1px -1px rgba(0, 0, 0, 0.12)'),
          }}
        />
      </Toolbar>
    );
  }
};
const ServiceTableToolbar = withTheme(ServiceTableToolbar_);

class ServiceTableHeader extends Component {
  constructor(props) {
    super(props);
    this.state = {
      sortedColumn: 'name',
      sortDirection: 'desc',
    };
  }

  sortBy = (column) => {
    const { sortedColumn } = this.state;
    let sortDirection = 'desc';
    if (sortedColumn === column) {
      sortDirection = (this.state.sortDirection === 'desc' ? 'asc' : 'desc');
      this.setState({
        sortDirection,
      });
    } else {
      this.setState({
        sortedColumn: column,
        sortDirection,
      });
    }
    this.props.sortBy(column, sortDirection);
  }

  render() {
    const { sortedColumn, sortDirection } = this.state;
    const { fields } = this.props;
    return (
      <TableHead>
        <TableRow>
          {
            fields.map(({ id, label, numeric }) => (
              <TableCell
                key={id}
                numeric={numeric}
              >
                <TableSortLabel
                  active={sortedColumn === id}
                  direction={sortDirection}
                  onClick={() => this.sortBy(id)}
                >
                  {label}
                </TableSortLabel>
              </TableCell>
            ))
          }
          <TableCell>Delete</TableCell>
        </TableRow>
      </TableHead>
    );
  }
};

const ServiceRow = ({ classes, service, fields, onClick, onDelete }) => {
  const { id } = service;
  return (
    <TableRow
      hover
      onClick={onClick}
    >
      {
        fields.map(({ id, numeric }) => (
          <TableCell
            key={`${id}-${id}`}
            numeric={numeric}
          >
            {service[id]}
          </TableCell>
        ))
      }
      <TableCell>
        <IconButton
          title="Delete service"
          onClick={(event) => {
            event.stopPropagation();
            onDelete(id);
          }}
          classes={{
            root: classes.deleteIcon,
          }}
        >
          <DeleteIcon />
        </IconButton>
      </TableCell>
    </TableRow>
  );
};

const ServiceTable = ({ fields, services, classes, history }) => (
  <Paper className={classes.container}>
    <ServiceTableToolbar
      onSearch={() => {}}
    />
    <Table>
      <ServiceTableHeader
        fields={fields}
        sortBy={() => {}}
      />
      <TableBody>
        {
          services.map((service) => (
            <ServiceRow
              key={service.id}
              fields={fields}
              service={service}
              onClick={() => history.push(`/services/${service.cluster}/${service.id}`)}
              onDelete={(id) => {}}
              classes={classes}
            />
          ))
        }
      </TableBody>
    </Table>
  </Paper>
);

export default withStyles((theme) => ({
  container: {
    height: '100%',
    width: '100%',
    overflow: 'auto',
    boxShadow: 'none !important',
    borderRadius: '0 !important',
  },
  deleteIcon: {
    color: (
      theme.palette.type === 'light'
      ? theme.palette.error[400]
      : theme.palette.error[300]
    ),
  }
}))(withRouter(ServiceTable));
