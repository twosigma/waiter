import React, { Component } from 'react';
import { BrowserRouter, Redirect, Route, Switch } from 'react-router-dom';
import SwipeableViews from 'react-swipeable-views';


import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import thunk from 'redux-thunk';
import { createLogger } from 'redux-logger';

import Theme from './theme';
import { reducer } from './reducer';

import Header from './header';
import { TabRoute } from './common';
import ServicesPage from './services/page';
import TokensPage from './tokens/page';

const logger = createLogger({
  collapsed: true,
});
const store = createStore(reducer,
                          applyMiddleware(thunk, logger));

const tabs = ['services', 'tokens']

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tabIndex: 0,
    };
  }

  render() {
    const { tabIndex } = this.state;
    return (
      <Provider store={store}>
        <Theme>
          <BrowserRouter>
            <div>
              <Header
                tabs={tabs}
                tabIndex={tabIndex}
                onTabChange={(tabIndex) => {
                  this.setState({ tabIndex });
                }}
              />
              <Switch>
                <Route
                  exact
                  path="/"
                  component={() => (
                    <Redirect to="/services" />
                  )}
                />
                {
                  tabs.map((tab, index) => (
                    <TabRoute
                      key={tab}
                      path={`/${tab}`}
                      onMount={() => this.setState({ tabIndex: index })}
                    />
                  ))
                }
              </Switch>
              <SwipeableViews
                index={tabIndex}
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
                <ServicesPage />
                <TokensPage />
              </SwipeableViews>
            </div>
          </BrowserRouter>
        </Theme>
      </Provider>
    );
  }
};

export default App;
