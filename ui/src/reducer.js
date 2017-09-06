import { Map } from 'immutable';
import { Actions } from './actions';

const initState = Map({
  settings: Map({
    theme: 'light',
  }),
});

const reducer = (state = initState, action) => {
  switch (action.type) {
    case Actions.SET_THEME: {
      return state.setIn(['settings', 'theme'], action.theme);
    }
    default: {
      return state;
    }
  };
};

export {
  reducer,
};
