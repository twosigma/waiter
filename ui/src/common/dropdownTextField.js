import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { MenuItem, Paper, TextField, withStyles } from 'material-ui';
import Autosuggest from 'react-autosuggest';

class DropdownTextField extends Component {
  constructor(props) {
    super(props);
    const { value, options } = this.props;
    this.state = {
      value: value || '',
      complete: true,
      suggestions: options,
    };
  }

  getSuggestions = (value) => {
    const lowerValue = value.toLowerCase();
    return this.props.options.filter((suggestion) => (
      suggestion.toLowerCase().indexOf(lowerValue) >= 0
    ));
  };

  onChange = (event, { newValue }) => {
    this.setState({
      value: newValue,
      complete: false,
      suggestions: this.getSuggestions(newValue),
    });
  };

  onClear = () => {
    this.setState({
      complete: true,
      suggestions: (this.state.value ?
                    this.state.suggestions :
                    this.props.options),
    });
  };

  shouldRenderSuggestions = () => {
    return this.state.suggestions.length;
  }

  renderSuggestion = (suggestion, { isHighlighted }) => (
    <MenuItem
      selected={isHighlighted}
      component="div"
    >
      {suggestion}
    </MenuItem>
  );

  renderInput = ({ classes, value, ref, ...other }) => (
    <TextField
      label={this.props.label}
      value={value}
      className={classes.textField}
      inputRef={ref}
      InputProps={{
        classes: {
          input: classes.input,
        },
        ...other,
      }}
    />
  );

  renderContainer = ({ containerProps, children }) => (
    <Paper
      {...containerProps}
      elevation={4}
    >
      {children}
    </Paper>
  );

  render() {
    const { classes } = this.props;
    const { value, suggestions } = this.state;

    const inputProps = {
      onChange: this.onChange,
      value,
      classes,
    };
    const theme = {
      suggestion: classes.suggestion,
      suggestionsList: classes.suggestionsList,
      container: classes.container,
      suggestionsContainerOpen: classes.suggestionsContainerOpen,
    };

    return (
      <Autosuggest
        theme={theme}
        suggestions={suggestions}
        inputProps={inputProps}
        getSuggestionValue={_ => _}
        renderInputComponent={this.renderInput}
        renderSuggestion={this.renderSuggestion}
        renderSuggestionsContainer={this.renderContainer}
        onSuggestionsFetchRequested={() => {}}
        onSuggestionsClearRequested={this.onClear}
        shouldRenderSuggestions={this.shouldRenderSuggestions}
        focusInputOnSuggestionClick={false}
      />
    );
  }
};

DropdownTextField.propTypes = {
  options: PropTypes.array.isRequired,
  value: PropTypes.string,
  label: PropTypes.string,
};

export default withStyles((theme) => ({
  container: {
    position: 'relative',
  },
  suggestionsContainerOpen: {
    position: 'absolute',
    marginTop: theme.spacing.unit,
    marginBottom: theme.spacing.unit * 3,
    left: 0,
    right: 0,
    overflow: 'auto',
  },
  suggestion: {
    display: 'block',
  },
  suggestionsList: {
    margin: 0,
    padding: 0,
    listStyleType: 'none',
  },
  textField: {
    width: '100%',
  },
}))(DropdownTextField);
