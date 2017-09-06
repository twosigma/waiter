import React, { Component } from 'react';
import { withTheme } from 'material-ui';
import AceEditor from 'react-ace';

import 'brace/mode/json';
import 'brace/theme/monokai';
import 'brace/theme/tomorrow';

const JsonEditor = ({ theme, className }) => (
  <AceEditor
    mode="json"
    theme={theme.palette.type === 'light' ? 'monokai' : 'tomorrow'}
    showGutter
    highlightActiveLine
    tabSize={2}
    width="100%"
    height="100%"
    editorProps={{
      $blockScrolling: Infinity,
    }}
    className={className}
  />
);

export default withTheme(JsonEditor);
