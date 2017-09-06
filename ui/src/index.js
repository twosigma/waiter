import React from 'react';
import ReactDOM from 'react-dom';
import { AppContainer } from 'react-hot-loader';

import App from './app';

const appDOM = document.getElementById('app');
const render = (Component) => (
  ReactDOM.render(
    <AppContainer>
      <Component />
    </AppContainer>,
    appDOM
  )
);

render(App);

if (module.hot) {
  module.hot.accept('./app',
                    () => render(App));
}
